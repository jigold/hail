import abc
import json
import logging
from shlex import quote as shq
from .constants import GITHUB_CLONE_URL, AUTHORIZED_USERS

from hailtop.utils import RETRY_FUNCTION_SCRIPT


log = logging.getLogger('github')


class Repo:
    def __init__(self, owner, name):
        assert isinstance(owner, str)
        assert isinstance(name, str)
        self.owner = owner
        self.name = name
        self.url = f'{GITHUB_CLONE_URL}{owner}/{name}.git'

    def __eq__(self, other):
        return self.owner == other.owner and self.name == other.name

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.owner, self.name))

    def __str__(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_short_str(s):
        pieces = s.split("/")
        assert len(pieces) == 2, f'{pieces} {s}'
        return Repo(pieces[0], pieces[1])

    def short_str(self):
        return f'{self.owner}/{self.name}'

    @staticmethod
    def from_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'owner' in d, d
        assert 'name' in d, d
        return Repo(d['owner'], d['name'])

    def to_dict(self):
        return {'owner': self.owner, 'name': self.name}

    @staticmethod
    def from_gh_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'owner' in d, d
        assert 'login' in d['owner'], d
        assert 'name' in d, d
        return Repo(d['owner']['login'], d['name'])


class FQBranch:
    def __init__(self, repo, name):
        assert isinstance(repo, Repo)
        assert isinstance(name, str)
        self.repo = repo
        self.name = name

    def __eq__(self, other):
        return self.repo == other.repo and self.name == other.name

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.repo, self.name))

    def __str__(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_short_str(s):
        pieces = s.split(":")
        assert len(pieces) == 2, f'{pieces} {s}'
        return FQBranch(Repo.from_short_str(pieces[0]), pieces[1])

    def short_str(self):
        return f'{self.repo.short_str()}:{self.name}'

    @staticmethod
    def from_gh_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'repo' in d, d
        assert 'ref' in d, d
        return FQBranch(Repo.from_gh_json(d['repo']), d['ref'])

    @staticmethod
    def from_json(d):
        assert isinstance(d, dict), f'{type(d)} {d}'
        assert 'repo' in d, d
        assert 'name' in d, d
        return FQBranch(Repo.from_json(d['repo']), d['name'])

    def to_dict(self):
        return {'repo': self.repo.to_dict(), 'name': self.name}


def clone_or_fetch_script(repo):
    return f"""
{ RETRY_FUNCTION_SCRIPT }

function clone() {{
    rm -rf ./{{*,.*}}
    git clone { shq(repo) } ./
}}

if [ ! -d .git ]; then
  time retry clone

  git config user.email ci@hail.is
  git config user.name ci
else
  git reset --hard
  time retry git fetch -q origin
fi
"""


class Code(abc.ABC):
    @abc.abstractmethod
    def short_str(self):
        pass

    @abc.abstractmethod
    def config(self):
        pass

    @abc.abstractmethod
    def repo_dir(self):
        """Path to repository on the ci (locally)."""

    @abc.abstractmethod
    def checkout_script(self):
        """Bash script to checkout out the code in the current directory."""


class PR(Code):
    def __init__(self, number, title, source_repo, source_sha, target_branch, author, labels):
        self.number = number
        self.title = title
        self.source_repo = source_repo
        self.source_sha = source_sha
        self.target_branch = target_branch
        self.author = author
        self.labels = labels
        self.sha = None

    async def authorized(self, dbpool):
        if self.author in AUTHORIZED_USERS:
            return True

        async with dbpool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT * from authorized_shas WHERE sha = %s;', self.source_sha)
                row = await cursor.fetchone()
                return row is not None

    def short_str(self):
        return f'pr-{self.number}'

    @staticmethod
    def from_gh_json(gh_json, target_branch):
        head = gh_json['head']
        return PR(gh_json['number'],
                  gh_json['title'],
                  Repo.from_gh_json(head['repo']),
                  head['sha'],
                  target_branch,
                  gh_json['user']['login'],
                  {label['name'] for label in gh_json['labels']})

    def repo_dir(self):
        return self.target_branch.repo_dir()

    def config(self):
        assert self.sha is not None
        target_repo = self.target_branch.branch.repo
        return {
            'checkout_script': self.checkout_script(),
            'number': self.number,
            'source_repo': self.source_repo.short_str(),
            'source_repo_url': self.source_repo.url,
            'source_sha': self.source_sha,
            'target_repo': target_repo.short_str(),
            'target_repo_url': target_repo.url,
            'target_sha': self.target_branch.sha,
            'sha': self.sha
        }

    def checkout_script(self):
        return f'''
{clone_or_fetch_script(self.target_branch.branch.repo.url)}

git remote add {shq(self.source_repo.short_str())} {shq(self.source_repo.url)} || true

time retry git fetch -q {shq(self.source_repo.short_str())}
git checkout {shq(self.target_branch.sha)}
git merge {shq(self.source_sha)} -m 'merge PR'
'''


class WatchedBranch(Code):
    def __init__(self, index, branch):
        self.index = index
        self.branch = branch
        self.prs = None
        self.sha = None

    def short_str(self):
        return f'br-{self.branch.repo.owner}-{self.branch.repo.name}-{self.branch.name}'

    def repo_dir(self):
        return f'repos/{self.branch.repo.short_str()}'

    def config(self):
        assert self.sha is not None
        return {
            'checkout_script': self.checkout_script(),
            'branch': self.branch.name,
            'repo': self.branch.repo.short_str(),
            'repo_url': self.branch.repo.url,
            'sha': self.sha
        }

    def checkout_script(self):
        return f'''
{clone_or_fetch_script(self.branch.repo.url)}

git checkout {shq(self.sha)}
'''


class UnwatchedBranch(Code):
    def __init__(self, branch, sha, user):
        self.branch = branch
        self.user = user
        self.sha = sha

    def short_str(self):
        return f'br-{self.branch.repo.owner}-{self.branch.repo.name}-{self.branch.name}'

    def repo_dir(self):
        return f'repos/{self.branch.repo.short_str()}'

    def config(self):
        return {
            'checkout_script': self.checkout_script(),
            'branch': self.branch.name,
            'repo': self.branch.repo.short_str(),
            'repo_url': self.branch.repo.url,
            'sha': self.sha,
            'user': self.user
        }

    def checkout_script(self):
        return f'''
{clone_or_fetch_script(self.branch.repo.url)}

git checkout {shq(self.sha)}
'''
