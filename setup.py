import setuptools

exec(open("trio_amqp/_version.py", encoding="utf-8").read())

description = 'AMQP implementation using trio'

setuptools.setup(
    name=__packagename__,  # noqa: F821
    version=__version__,  # noqa: F821
    author="Matthias Urlichs",
    author_email='matthias@urlichs.de',
    url='https://github.com/python-trio/trio-amqp',
    description=description,
    long_description=open('README.rst').read(),
    download_url='https://pypi.python.org/pypi/trio_amqp',
    setup_requires=[
        'pyrabbit',
        'pytest-trio >= 0.3',
    ],
    install_requires=[
        'trio',
    ],
    packages=[
        'trio_amqp',
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
