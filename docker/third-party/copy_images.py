import argparse
import asyncio
from typing import List

from hailtop.utils import OnlineBoundedGather2, CalledProcessError, sync_check_shell, sync_check_shell_output


async def copy_image(image: str, dest: str):
    try:
        sync_check_shell_output('command -v skopeo')
    except CalledProcessError:
        sync_check_shell(f'docker pull {image}')
        sync_check_shell(f'docker tag {image} {dest}')
        sync_check_shell(f'docker push {dest}')
    else:
        sync_check_shell(f'skopeo copy --override-os linux --override-arch amd64 docker://docker.io/{image} docker://{dest}')


async def copy_images(docker_prefix: str,
                      images: List[str],
                      parallelism: int = 3):
    assert docker_prefix
    sema = asyncio.Semaphore(parallelism)
    async with OnlineBoundedGather2(sema) as pool:
        tasks = [await pool.call(copy_image, image, f'{docker_prefix}/{image}') for image in images]
        if tasks:
            await pool.wait(tasks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--docker-prefix', type=str, required=True)
    parser.add_argument('--images', type=str, required=True)
    parser.add_argument('--parallelism', type=int, required=False, default=3)
    args = parser.parse_args()

    image_names = []
    with open(args.images, 'r') as f:
        for line in f:
            image_names.append(line.rstrip())

    asyncio.run(copy_images(args.docker_prefix, image_names, args.parallelism))
