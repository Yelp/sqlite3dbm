from sqlite3dbm import __version__

import setuptools
setuptools.setup(
    author='Jason Fennell',
    author_email='jfennell@yelp.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    description='sqlite-backed dictionary',
    license='Apache',
    long_description=open('README.md').read(),
    name='sqlite3dbm',
    packages=['sqlite3dbm'],
    provides=['sqlite3dbm'],
    url='http://github.com/Yelp/sqlite3dbm/',
    version=__version__,
)
