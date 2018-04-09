# Copyright 2015 SAP SE.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http: //www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

from setuptools import setup

setup(
    name="PySnappydata",
    version="0.0.0.1",
    description="Python interface to snappydata",
    license="Apache License Version 2.0",
    url="https://github.com/deatheye/PySnappydata/",
    author="Yu Yan",
    author_email="coderyy@163.com",
    packages=['pysnappydata', 'SDTCLIService'],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=[
        'future',
    ],
    extras_require={
        "SQLAlchemy": ['sqlalchemy>=0.5.0'],
    },
    tests_require=[
        'mock>=1.0.0',
        'pytest',
        'pytest-cov',
        'sqlalchemy>=0.5.0',
        'thrift>=0.8.0',
    ],
    zip_safe=False,
     entry_points = {  
         'sqlalchemy.dialects': 
         ['snappydata = pysnappydata.sqlalchemy_snappydata:SnappyDataDialect']
     },
)
