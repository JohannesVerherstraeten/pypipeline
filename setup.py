#!/usr/bin/env python
# Copyright (C) 2021  Johannes Verherstraeten
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see https://www.gnu.org/licenses/agpl-3.0.en.html

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
extras['dev'] = extras['docs'] + extras['ray'] + extras['tests'] + extras['serve']
extras['all'] = extras['dev'] + extras['lib_torch']

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
