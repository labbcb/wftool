from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='wftool',
    version='1.0.0',
    author="Welliton Souza",
    author_email="well309@gmail.com",
    description="Task and workflow management for genomics research",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/labbcb/wftool",
    packages=find_packages(),
    install_requires=[
        'Click', 'requests', 'validators'
    ],
    entry_points='''
        [console_scripts]
        wftool=wftool.scripts.wftool:cli
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
