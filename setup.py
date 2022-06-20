#!/usr/bin/env python
# Copyright 2021 Johannes Verherstraeten
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List
from pathlib import Path
from setuptools import setup
from pkg_resources import parse_requirements


_PATH_ROOT = Path(__file__).parent
_PATH_REQUIREMENTS = _PATH_ROOT / 'requirements'


def read(fname: str) -> str:
    return open(_PATH_ROOT / fname).read()


def read_requirements(req_path: Path) -> List[str]:
    assert req_path.exists()
    with open(req_path, "r") as f:
        res = parse_requirements(f.read())
    return [str(r) for r in res]


# https://setuptools.readthedocs.io/en/latest/pkg_resources.html#requirements-parsing
# For installing package extras: `pip install pypipeline[ray, tests]`
# If you have the repo cloned locally: `pip install ".[ray, tests]"`
extras = {
    'docs': read_requirements(_PATH_REQUIREMENTS / "docs.txt"),
    'ray': read_requirements(_PATH_REQUIREMENTS / "ray.txt"),
    'tests': read_requirements(_PATH_REQUIREMENTS / "tests.txt"),
    'lib_torch': read_requirements(_PATH_REQUIREMENTS / "lib_torch.txt"),
    'serve': read_requirements(_PATH_REQUIREMENTS / "serve.txt")
}
extras["tests"] = extras["tests"] + extras['ray'] + extras['lib_torch'] + extras['serve']
extras['all'] = extras['tests'] + extras['docs']

setup(
    name="pypipeline",
    version="0.1.0",
    author="Johannes Verherstraeten",
    author_email="johannes.verherstraeten@hotmail.com",
    description="Encapsulate computations, combine them to algorithms, enable pipeline parallelism and scale up. ",
    keywords="pipeline pipelining parallelism parallelization scaling ray threading algorithm throughput building "
             "blocks",
    python_requires='>=3.7,<3.9',
    setup_requires=[],
    install_requires=[],
    extras_require=extras,
    url="https://github.com/JohannesVerherstraeten/pypipeline",
    packages=['pypipeline', 'pypipeline_lib', 'pypipeline_serve'],
    long_description=read('README.md'),
    classifiers=[
        'Environment :: Console',
        'Natural Language :: English',
        # Project maturity
        "Development Status :: 3 - Alpha",
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: Scientific/Engineering :: Image Recognition',
        'Topic :: Scientific/Engineering :: Information Analysis',
        # License
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: OS Independent',
        # Supported Python versions
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
