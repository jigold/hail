import logging
import secrets
from shlex import quote as shq
import gear.github

from hailtop.utils import check_shell, check_shell_output
from hailtop.batch_client.aioclient import Batch

log = logging.getLogger('benchmark')


class WatchedBranch(gear.github.WatchedBranch):
    def __init__(self, index, branch, deployable):
        super().__init__(index, branch)

        self.deployable = deployable

        # success, failure, pending
        self.deploy_batch = None
        self._deploy_state = None

        self.updating = False
        self.github_changed = True
        self.batch_changed = True
        self.state_changed = True

        self.n_running_batches = None

    @property
    def deploy_state(self):
        return self._deploy_state

    @deploy_state.setter
    def deploy_state(self, new_state):
        self._deploy_state = new_state

    async def notify_github_changed(self, app):
        self.github_changed = True
        await self._update(app)

    async def notify_batch_changed(self, app):
        self.batch_changed = True
        await self._update(app)

    async def update(self, app):
        # update everything
        self.github_changed = True
        self.batch_changed = True
        self.state_changed = True
        await self._update(app)

    async def _update(self, app):
        if self.updating:
            log.info(f'already updating {self.short_str()}')
            return

        try:
            log.info(f'start update {self.short_str()}')
            self.updating = True
            gh = app['github_client']
            batch_client = app['batch_client']
            dbpool = app['dbpool']

            while self.github_changed or self.batch_changed or self.state_changed:
                if self.github_changed:
                    self.github_changed = False
                    await self._update_github(gh)

                if self.batch_changed:
                    self.batch_changed = False
                    await self._update_batch(batch_client, dbpool)

                if self.state_changed:
                    self.state_changed = False
                    await self._heal(batch_client, dbpool, gh)
                    await self.try_to_merge(gh)
        finally:
            log.info(f'update done {self.short_str()}')
            self.updating = False

    async def try_to_merge(self, gh):
        for pr in self.prs.values():
            if pr.is_mergeable():
                if await pr.merge(gh):
                    self.github_changed = True
                    self.sha = None
                    self.state_changed = True
                    return

    async def _update_github(self, gh):
        log.info(f'update github {self.short_str()}')

        repo_ss = self.branch.repo.short_str()

        branch_gh_json = await gh.getitem(f'/repos/{repo_ss}/git/refs/heads/{self.branch.name}')
        new_sha = branch_gh_json['object']['sha']
        if new_sha != self.sha:
            log.info(f'{self.branch.short_str()} sha changed: {self.sha} => {new_sha}')
            self.sha = new_sha
            self.state_changed = True

        new_prs = {}
        async for gh_json_pr in gh.getiter(f'/repos/{repo_ss}/pulls?state=open&base={self.branch.name}'):
            number = gh_json_pr['number']
            if self.prs is not None and number in self.prs:
                pr = self.prs[number]
                pr.update_from_gh_json(gh_json_pr)
            else:
                pr = PR.from_gh_json(gh_json_pr, self)
            new_prs[number] = pr
        self.prs = new_prs

        for pr in new_prs.values():
            await pr._update_github(gh)

    async def _update_deploy(self, batch_client):
        assert self.deployable

        if self.deploy_state:
            assert self.deploy_batch
            return

        if self.deploy_batch is None:
            running_deploy_batches = batch_client.list_batches(
                f'!complete deploy=1 target_branch={self.branch.short_str()}')
            running_deploy_batches = [b async for b in running_deploy_batches]
            if running_deploy_batches:
                self.deploy_batch = max(running_deploy_batches, key=lambda b: b.id)
            else:
                deploy_batches = batch_client.list_batches(
                    f'deploy=1 '
                    f'target_branch={self.branch.short_str()} '
                    f'sha={self.sha}')
                deploy_batches = [b async for b in deploy_batches]
                if deploy_batches:
                    self.deploy_batch = max(deploy_batches, key=lambda b: b.id)

        if self.deploy_batch:
            try:
                status = await self.deploy_batch.status()
            except aiohttp.client_exceptions.ClientResponseError as exc:
                log.info(f'Could not update deploy_batch status due to exception {exc}, setting deploy_batch to None')
                self.deploy_batch = None
                return
            if status['complete']:
                if status['state'] == 'success':
                    self.deploy_state = 'success'
                else:
                    self.deploy_state = 'failure'

                if not is_test_deployment and self.deploy_state == 'failure':
                    url = deploy_config.external_url(
                        'ci',
                        f'/batches/{self.deploy_batch.id}')
                    request = {
                        'type': 'stream',
                        'to': 'team',
                        'topic': 'CI Deploy Failure',
                        'content': f'''
@*dev*
state: {self.deploy_state}
branch: {self.branch.short_str()}
sha: {self.sha}
url: {url}
'''}
                    result = zulip_client.send_message(request)
                    log.info(result)

                self.state_changed = True

    async def _heal_deploy(self, batch_client):
        assert self.deployable

        if not self.sha:
            return

        if (self.deploy_batch is None
                or (self.deploy_state and self.deploy_batch.attributes['sha'] != self.sha)):
            async with repos_lock:
                await self._start_deploy(batch_client)

    async def _update_batch(self, batch_client, dbpool):
        log.info(f'update batch {self.short_str()}')

        if self.deployable:
            await self._update_deploy(batch_client)

        for pr in self.prs.values():
            await pr._update_batch(batch_client, dbpool)

    async def _heal(self, batch_client, dbpool, gh):
        log.info(f'heal {self.short_str()}')

        if self.deployable:
            await self._heal_deploy(batch_client)

        merge_candidate = None
        merge_candidate_pri = None
        for pr in self.prs.values():
            # merge candidate if up-to-date build passing, or
            # pending but haven't failed
            if (pr.review_state == 'approved'
                    and (pr.build_state == 'success' or not pr.source_sha_failed)):
                pri = pr.merge_priority()
                is_authorized = await pr.authorized(dbpool)
                if is_authorized and (not merge_candidate or pri > merge_candidate_pri):
                    merge_candidate = pr
                    merge_candidate_pri = pri
        if merge_candidate:
            log.info(f'merge candidate {merge_candidate.number}')

        self.n_running_batches = sum(1 for pr in self.prs.values() if pr.batch and not pr.build_state)

        for pr in self.prs.values():
            await pr._heal(batch_client, dbpool, pr == merge_candidate, gh)

        # cancel orphan builds
        running_batches = batch_client.list_batches(
            f'!complete test=1 target_branch={self.branch.short_str()}')
        seen_batch_ids = set(pr.batch.id for pr in self.prs.values() if pr.batch and hasattr(pr.batch, 'id'))
        async for batch in running_batches:
            if batch.id not in seen_batch_ids:
                attrs = batch.attributes
                log.info(f'cancel batch {batch.id} for {attrs["pr"]} {attrs["source_sha"]} => {attrs["target_sha"]}')
                await batch.cancel()

    async def _start_deploy(self, batch_client):
        # not deploying
        assert not self.deploy_batch or self.deploy_state

        self.deploy_batch = None
        self.deploy_state = None

        deploy_batch = None
        try:
            repo_dir = self.repo_dir()
            await check_shell(f'''
mkdir -p {shq(repo_dir)}
(cd {shq(repo_dir)}; {self.checkout_script()})
''')
            with open(f'{repo_dir}/build.yaml', 'r') as f:
                config = BuildConfiguration(self, f.read(), scope='deploy')

            log.info(f'creating deploy batch for {self.branch.short_str()}')
            deploy_batch = batch_client.create_batch(
                attributes={
                    'token': secrets.token_hex(16),
                    'deploy': '1',
                    'target_branch': self.branch.short_str(),
                    'sha': self.sha
                },
                callback=CALLBACK_URL)
            config.build(deploy_batch, self, scope='deploy')
            deploy_batch = await deploy_batch.submit()
            self.deploy_batch = deploy_batch
        except concurrent.futures.CancelledError:
            raise
        except Exception as e:  # pylint: disable=broad-except
            log.exception('could not start deploy')
            self.deploy_batch = MergeFailureBatch(
                e,
                attributes={
                    'deploy': '1',
                    'target_branch': self.branch.short_str(),
                    'sha': self.sha
                })
            self.deploy_state = 'checkout_failure'
        finally:
            if deploy_batch and not self.deploy_batch:
                log.info(f'cancelling partial deploy batch {deploy_batch.id}')
                await deploy_batch.cancel()



class UnwatchedBranch(gear.github.UnwatchedBranch):
    def __init__(self, branch, sha, userdata):
        super().__init__(branch, sha, userdata['username'])

        self.deploy_batch = None

    async def deploy(self, batch_client):
        assert not self.deploy_batch

        deploy_batch = None
        try:
            repo_dir = self.repo_dir()
            await check_shell(f'''
mkdir -p {shq(repo_dir)}
(cd {shq(repo_dir)}; {self.checkout_script()})
''')

            log.info(f'creating dev deploy batch for {self.branch.short_str()} and user {self.user}')

            deploy_batch = batch_client.create_batch(
                attributes={
                    'token': secrets.token_hex(16),
                    'target_branch': self.branch.short_str(),
                    'sha': self.sha,
                    'user': self.user
                })

            # TODO: Add jobs to actually run the benchmarks here!

            deploy_batch = await deploy_batch.submit()
            self.deploy_batch = deploy_batch
            return deploy_batch.id
        finally:
            if deploy_batch and not self.deploy_batch and isinstance(deploy_batch, Batch):
                log.info(f'cancelling partial deploy batch {deploy_batch.id}')
                await deploy_batch.cancel()
