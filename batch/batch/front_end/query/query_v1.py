from typing import Any, List, Optional, Tuple

from ...exceptions import QueryError
from .query import QueryType, job_state_search_term_to_states


def parse_list_batches_query_v1(user: str, q: str, last_batch_id: Optional[int]) -> Tuple[str, List[Any]]:
    where_conditions = [
        '(billing_project_users.`user` = %s AND billing_project_users.billing_project = batches.billing_project)',
        'NOT deleted',
    ]
    where_args: List[Any] = [user]

    attributes_lateral_join_conditions: List[str] = []
    attributes_lateral_join_args: List[Any] = []

    if last_batch_id is not None:
        where_conditions.append('(batches.id < %s)')
        where_args.append(last_batch_id)

    terms = q.split()
    for _t in terms:
        if _t[0] == '!':
            negate = True
            t = _t[1:]
        else:
            negate = False
            t = _t

        if '=' in t:
            k, v = t.split('=', 1)
            condition = """
((batches.id) IN
 (SELECT batch_id FROM job_group_attributes
  WHERE `key` = %s AND `value` = %s))
"""
            args = [k, v]
            query_type = QueryType.ATTRIBUTES_LATERAL_JOIN
        elif t.startswith('has:'):
            k = t[4:]
            condition = """
((batches.id) IN
 (SELECT batch_id FROM job_group_attributes
  WHERE `key` = %s))
"""
            args = [k]
            query_type = QueryType.ATTRIBUTES_LATERAL_JOIN
        elif t.startswith('user:'):
            k = t[5:]
            condition = """
(batches.`user` = %s)
"""
            args = [k]
            query_type = QueryType.WHERE
        elif t.startswith('billing_project:'):
            k = t[16:]
            condition = """
(billing_projects.name_cs = %s)
"""
            args = [k]
            query_type = QueryType.WHERE
        elif t == 'open':
            condition = "(`state` = 'open')"
            args = []
            query_type = QueryType.WHERE
        elif t == 'closed':
            condition = "(`state` != 'open')"
            args = []
            query_type = QueryType.WHERE
        elif t == 'complete':
            condition = "(`state` = 'complete')"
            args = []
            query_type = QueryType.WHERE
        elif t == 'running':
            condition = "(`state` = 'running')"
            args = []
            query_type = QueryType.WHERE
        elif t == 'cancelled':
            condition = '(job_groups_cancelled.id IS NOT NULL)'
            args = []
            query_type = QueryType.WHERE
        elif t == 'failure':
            condition = '(n_failed > 0)'
            args = []
            query_type = QueryType.WHERE
        elif t == 'success':
            # need complete because there might be no jobs
            condition = "(`state` = 'complete' AND n_succeeded = n_jobs)"
            args = []
            query_type = QueryType.WHERE
        else:
            raise QueryError(f'Invalid search term: {t}.')

        if negate:
            condition = f'(NOT {condition})'

        if query_type == QueryType.WHERE:
            where_conditions.append(condition)
            where_args.extend(args)
        else:
            assert query_type == QueryType.ATTRIBUTES_LATERAL_JOIN
            attributes_lateral_join_conditions.append(condition)
            attributes_lateral_join_args.extend(args)

    if attributes_lateral_join_conditions:
        attributes_lateral_join_conditions_str = 'AND ' + ' AND '.join(attributes_lateral_join_conditions)
    else:
        attributes_lateral_join_conditions_str = ''

    sql = f"""
WITH base_t AS (
  SELECT batches.*,
    job_groups_cancelled.id IS NOT NULL AS cancelled,
    job_groups_n_jobs_in_complete_states.n_completed,
    job_groups_n_jobs_in_complete_states.n_succeeded,
    job_groups_n_jobs_in_complete_states.n_failed,
    job_groups_n_jobs_in_complete_states.n_cancelled
  FROM batches
  LEFT JOIN billing_projects ON batches.billing_project = billing_projects.name
  LEFT JOIN job_groups_n_jobs_in_complete_states
    ON batches.id = job_groups_n_jobs_in_complete_states.id
  LEFT JOIN job_groups_cancelled
    ON batches.id = job_groups_cancelled.id
  LEFT JOIN LATERAL (
    SELECT 1
    FROM job_group_attributes
    WHERE job_group_attributes.batch_id = batches.id {attributes_lateral_join_conditions_str}
  ) AS jga ON TRUE
  STRAIGHT_JOIN billing_project_users ON batches.billing_project = billing_project_users.billing_project
  WHERE {' AND '.join(where_conditions)}
  ORDER BY id DESC
  LIMIT 51
)
SELECT base_t.*, cost_t.cost, cost_t.cost_breakdown
FROM base_t
LEFT JOIN LATERAL (
  SELECT COALESCE(SUM(`usage` * rate), 0) AS cost, JSON_OBJECTAGG(resources.resource, COALESCE(`usage` * rate, 0)) AS cost_breakdown
  FROM (
    SELECT batch_id, resource_id, CAST(COALESCE(SUM(`usage`), 0) AS SIGNED) AS `usage`
    FROM aggregated_job_group_resources_v3
    WHERE base_t.id = aggregated_job_group_resources_v3.batch_id
    GROUP BY batch_id, resource_id
  ) AS usage_t
  LEFT JOIN resources ON usage_t.resource_id = resources.resource_id
  GROUP BY batch_id
) AS cost_t ON TRUE
ORDER BY id DESC;
"""

    return (sql, where_args)


def parse_batch_jobs_query_v1(batch_id: int, q: str, last_job_id: Optional[int]) -> Tuple[str, List[Any]]:
    # batch has already been validated
    where_conditions = ['(jobs.batch_id = %s AND batch_updates.committed)']
    where_args: List[Any] = [batch_id]

    attributes_lateral_join_conditions: List[str] = []
    attributes_lateral_join_args: List[Any] = []

    if last_job_id is not None:
        where_conditions.append('(jobs.job_id > %s)')
        where_args.append(last_job_id)

    terms = q.split()
    for _t in terms:
        if _t[0] == '!':
            negate = True
            t = _t[1:]
        else:
            negate = False
            t = _t

        args: List[Any]

        if '=' in t:
            k, v = t.split('=', 1)
            if k == 'job_id':
                condition = '(jobs.job_id = %s)'
                args = [v]
                query_type = QueryType.WHERE
            else:
                condition = '(`key` = %s AND `value` = %s)'
                args = [k, v]
                query_type = QueryType.ATTRIBUTES_LATERAL_JOIN
        elif t.startswith('has:'):
            k = t[4:]
            condition = '(`key` = %s)'
            args = [k]
            query_type = QueryType.ATTRIBUTES_LATERAL_JOIN
        elif t in job_state_search_term_to_states:
            values = job_state_search_term_to_states[t]
            condition = ' OR '.join(['(jobs.state = %s)' for _ in values])
            condition = f'({condition})'
            args = [v.value for v in values]
            query_type = QueryType.WHERE
        else:
            raise QueryError(f'Invalid search term: {t}.')

        if negate:
            condition = f'(NOT {condition})'

        if query_type == QueryType.WHERE:
            where_conditions.append(condition)
            where_args.extend(args)
        else:
            assert query_type == QueryType.ATTRIBUTES_LATERAL_JOIN
            attributes_lateral_join_conditions.append(condition)
            attributes_lateral_join_args.extend(args)

    if attributes_lateral_join_conditions:
        attributes_lateral_join_conditions_str = 'AND ' + ' AND '.join(attributes_lateral_join_conditions)
    else:
        attributes_lateral_join_conditions_str = ''

    sql = f"""
WITH base_t AS
(
  SELECT jobs.*, batches.user, batches.billing_project, batches.format_version,
    job_attributes.value AS name
  FROM jobs
  INNER JOIN batches ON jobs.batch_id = batches.id
  INNER JOIN batch_updates ON jobs.batch_id = batch_updates.batch_id AND jobs.update_id = batch_updates.update_id
  LEFT JOIN job_attributes
  ON jobs.batch_id = job_attributes.batch_id AND
    jobs.job_id = job_attributes.job_id AND
    job_attributes.`key` = 'name'
  LEFT JOIN LATERAL (
    SELECT 1
    FROM job_attributes
    WHERE job_attributes.batch_id = jobs.batch_id AND job_attributes.job_group_id = jobs.job_group_id {attributes_lateral_join_conditions_str}
  ) AS ja ON TRUE
  WHERE {' AND '.join(where_conditions)}
  LIMIT 50
)
SELECT base_t.*, cost_t.cost, cost_t.cost_breakdown
FROM base_t
LEFT JOIN LATERAL (
SELECT COALESCE(SUM(`usage` * rate), 0) AS cost, JSON_OBJECTAGG(resources.resource, COALESCE(`usage` * rate, 0)) AS cost_breakdown
FROM (SELECT aggregated_job_resources_v3.batch_id, aggregated_job_resources_v3.job_id, resource_id, CAST(COALESCE(SUM(`usage`), 0) AS SIGNED) AS `usage`
  FROM aggregated_job_resources_v3
  WHERE aggregated_job_resources_v3.batch_id = base_t.batch_id AND aggregated_job_resources_v3.job_id = base_t.job_id
  GROUP BY aggregated_job_resources_v3.batch_id, aggregated_job_resources_v3.job_id, aggregated_job_resources_v3.resource_id
) AS usage_t
LEFT JOIN resources ON usage_t.resource_id = resources.resource_id
GROUP BY usage_t.batch_id, usage_t.job_id
) AS cost_t ON TRUE;
"""

    return (sql, where_args)
