import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.txt')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.txt')) as f:
    CHANGES = f.read()

requires = [
    'automatron',
]

setup(
    name='automatron-redis',
    version='1.0.0',
    description='Automatron IRC bot - Redis configuration manager',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Twisted",
        "Topic :: Internet :: IRC",
    ],
    author='Ingmar Steen',
    author_email='iksteen@gmail.com',
    url='',
    keywords='',
    packages=find_packages() + ['twisted.plugins'],
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
)
