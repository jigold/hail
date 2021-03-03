import math
import sys
import os
import shutil

from typing import List, Optional

from ..utils.utils import grouped, digits_needed
from ..utils.process import sync_check_shell_output
from .exceptions import BatchException
from . import batch as _batch  # pylint: disable=cyclic-import
from .resource import ResourceGroup, ResourceFile


def build_python_image(build_dir: str,
                       dest_image: str,
                       python_requirements: Optional[List[str]] = None,
                       verbose: bool = True):

    version = sys.version_info
    if version.major != 3 or version.minor not in (6, 7, 8):
        raise ValueError(
            f'You must specify an image if you are using a Python version other than 3.6, 3.7, or 3.8 (you are using {version})')
    base_image = f'python:{version.major}.{version.minor}'

    docker_path = f'{build_dir}/docker'
    shutil.rmtree(docker_path)
    os.makedirs(docker_path)

    python_requirements = python_requirements or []
    python_requirements_file = f'{docker_path}/requirements.txt'
    with open(python_requirements_file, 'w') as f:
        f.write('\n'.join(python_requirements) + '\n')

    source_dir = '/Users/jigold/hail/hail/python'
    os.system(f'cp -R {source_dir} {docker_path}')

    with open(f'{docker_path}/Dockerfile', 'w') as f:
        f.write(f'''
FROM {base_image}

COPY requirements.txt .

RUN pip install --upgrade --no-cache-dir -r requirements.txt && \
    python3 -m pip check

COPY python /python/

RUN pip install /python/
''')

    sync_check_shell_output(f'docker build -t {dest_image} {docker_path}', echo=verbose)

    image_split = dest_image.rsplit('/', 1)
    if len(image_split) == 2:
        sync_check_shell_output(f'docker push {dest_image}', echo=verbose)


def concatenate(b: '_batch.Batch', files: List[ResourceFile], image: str = None, branching_factor: int = 100) -> ResourceFile:
    """
    Concatenate files using tree aggregation.

    Examples
    --------
    Create and execute a batch that concatenates output files:

    >>> b = Batch()
    >>> j1 = b.new_job()
    >>> j1.command(f'touch {j1.ofile}')
    >>> j2 = b.new_job()
    >>> j2.command(f'touch {j2.ofile}')
    >>> j3 = b.new_job()
    >>> j3.command(f'touch {j3.ofile}')
    >>> files = [j1.ofile, j2.ofile, j3.ofile]
    >>> ofile = concatenate(b, files, branching_factor=2)
    >>> b.run()

    Parameters
    ----------
    b:
        Batch to add concatenation jobs to.
    files:
        List of files to concatenate.
    branching_factor:
        Grouping factor when concatenating files.
    image:
        Image to use. Must have the cat command.

    Returns
    -------
    Concatenated output file.
    """

    def _concatenate(b, name, xs):
        j = b.new_job(name=name)
        if image:
            j.image(image)
        j.command(f'cat {" ".join(xs)} > {j.ofile}')
        return j.ofile

    if len(files) == 0:
        raise BatchException('Must have at least one file to concatenate.')

    if not all([isinstance(f, ResourceFile) for f in files]):
        raise BatchException('Invalid input file(s) - all inputs must be resource files.')

    return _combine(_concatenate, b, 'concatenate', files, branching_factor=branching_factor)


def plink_merge(b: '_batch.Batch', bfiles: List[ResourceGroup],
                image: str = None, branching_factor: int = 100) -> ResourceGroup:
    """
    Merge binary PLINK files using tree aggregation.

    Parameters
    ----------
    b:
        Batch to add merge jobs to.
    bfiles:
        List of binary PLINK file roots to merge.
    image:
        Image name that contains PLINK.
    branching_factor:
        Grouping factor when merging files.

    Returns
    -------
    Merged binary PLINK file.
    """

    def _plink_merge(b, name, xs):
        assert xs
        if len(xs) == 1:
            return xs[0]
        j = b.new_job(name=name)
        if image:
            j.image(image)
        for f in xs[1:]:
            j.command(f'echo "{f.bed} {f.bim} {f.fam}" >> merge_list')
        j.command(f'plink --bfile {xs[0]} --merge-list merge_list --out {j.ofile}')
        return j.ofile

    if len(bfiles) == 0:
        raise BatchException('Must have at least one binary PLINK file to merge.')

    if not all([isinstance(bf, ResourceGroup) for bf in bfiles]):
        raise BatchException('Invalid input file(s) - all inputs must be resource groups.')

    return _combine(_plink_merge, b, 'plink-merge', bfiles, branching_factor=branching_factor)


def _combine(combop, b, name, xs, branching_factor=100):
    assert isinstance(branching_factor, int) and branching_factor >= 1
    n_levels = math.ceil(math.log(len(xs), branching_factor))
    level_digits = digits_needed(n_levels)

    level = 0
    while level < n_levels:
        branch_digits = digits_needed(len(xs) // branching_factor + min(len(xs) % branching_factor, 1))
        grouped_xs = grouped(branching_factor, xs)
        xs = [combop(b, f'{name}-{level:0{level_digits}}-{i:0{branch_digits}}', xs) for i, xs in enumerate(grouped_xs)]
        level += 1
    assert len(xs) == 1
    return xs[0]
