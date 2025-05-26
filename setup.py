from setuptools import setup, find_packages

setup(
    name="tableau_application",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "tableauserverclient",
        "python-dotenv>=0.19.0",
    ],
    python_requires=">=3.7",
    author="Your Name",
    author_email="your.email@example.com",
    description="Tableau Application for managing projects and workbooks",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
) 