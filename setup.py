import setuptools

description = 'AMQP implementation using anyio'

setuptools.setup(
    name="asyncamqp",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    author="Matthias Urlichs",
    author_email='matthias@urlichs.de',
    url='https://github.com/python-trio/asyncamqp',
    description=description,
    long_description=open('README.rst').read(),
    download_url='https://pypi.python.org/pypi/asyncamqp',
    setup_requires=[
        'pyrabbit',
    ],
    install_requires=[
        'anyio',
    ],
    packages=[
        'asyncamqp',
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Framework :: Trio",
    ],
    platforms='all',
    license='BSD'
)
