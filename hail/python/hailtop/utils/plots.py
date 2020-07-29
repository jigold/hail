from .time import humanize_timedelta_msecs, time_msecs_str


def attempts_timeline(attempts):
    colors = {
        ('input', 'pulling'): '#ef7fa1',
        ('input', 'creating'): '#d31b53',
        ('input', 'starting'): '#ef7fa1',
        ('input', 'running'): '#d31b53',
        ('input', 'uploading_log'): '#ef7fa1',
        ('input', 'deleting'): '#d31b53',

        ('main', 'pulling'): '#2be47a',
        ('main', 'creating'): '#107f40',
        ('main', 'starting'): '#2be47a',
        ('main', 'running'): '#107f40',
        ('main', 'uploading_log'): '#2be47a',
        ('main', 'deleting'): '#107f40',

        ('output', 'pulling'): '#805ef9',
        ('output', 'creating'): '#3308d0',
        ('output', 'starting'): '#805ef9',
        ('output', 'running'): '#3308d0',
        ('output', 'uploading_log'): '#805ef9',
        ('output', 'deleting'): '#3308d0'
    }

    cols = [{'id': 'Attempt', 'type': 'string'},
            {'id': 'Task', 'type': 'string'},
            {'role': 'style', 'type': 'string'},
            {'role': 'tooltip', 'type': 'string', 'p': {'html': 'true'}},
            {'id': 'Start', 'type': 'number'},
            {'id': 'End', 'type': 'number'}]

    def parse_attempt(attempt, container, timings):
        attempt_rows = []
        for name, timing in timings.items():
            d = []
            d.append({'v': attempt})
            d.append({'v': f'{container}/{name}'})
            d.append({'v': colors[(container, name)]})
            d.append({'v': f'''
    <div style="padding:5px 5px 5px 5px;">
    <strong><span>{container}/{name}</span></strong>
    <br>
    <hr>
    <span><strong>Start:</strong> {time_msecs_str(timing['start_time'])}</span>
    <br>
    <span><strong>End:</strong> {time_msecs_str(timing['finish_time'])}</span>
    <br>
    <span><strong>Duration:</strong> {humanize_timedelta_msecs(timing['finish_time'] - timing['start_time'])}</span>
    <hr>
    </div>
    '''})
            d.append({'v': timing['start_time']})
            d.append({'v': timing['finish_time']})

            attempt_rows.append({'c': d})
        return attempt_rows

    rows = []

    for status in attempts:
        attempt_id = status['attempt_id']
        container_statuses = status['container_statuses']
        for container in ('input', 'main', 'output'):
            if container in container_statuses:
                rows.extend(parse_attempt(attempt_id, container, container_statuses[container]['timing']))

    data = {'cols': cols,
            'rows': rows}

    options = {
        'tooltip': {'isHtml': 'true'}
    }

    return (data, options)
