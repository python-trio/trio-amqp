import setuptools

description = 'AMQP implementation using anyio'

setuptools.setup(
    name="async_amqp",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    author="Matthias Urlichs",
    author_email='matthias@urlichs.de',
    url='https://github.com/python-trio/async_amqp',
    description=description,
    long_description=open('README.rst').read(),
    # download_url='https://pypi.python.org/pypi/async_amqp',
    setup_requires=[
        'pyrabbit',
    ],
    keywords=['asyncio', 'amqp', 'rabbitmq', 'aio'],
    packages=[
        'async_amqp',
    ],
    install_requires=[
        'pamqp>=2.2.0,<3',
        'anyio>=2',
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Framework :: Trio",
        "Framework :: Anyio",
    ],
    platforms='all',
    license='BSD'
)
