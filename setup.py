from setuptools import find_packages, setup

with open("readme.md", "r") as desc:
    long_description = desc.read()


setup(
    name="data_utils",
    version="0.2",
    author="Shawn-Philippe Levasseur",
    author_email="shawnphilippe.levasseur@decathlon.com",
    description="An s3 utility package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dktunited/data-utils",
    packages=find_packages(exclude=["tests"]),
    python_requires=">=3.6",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "boto3",
        "django-import-export",
        "numpy",
        "pandas",
        "tablib"
    ]
)
