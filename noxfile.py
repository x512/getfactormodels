import nox


@nox.session(python=["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"])
def tests(session):
    session.install("-r", "requirements.txt")
    session.install("pytest", "coverage", "pytest-cov", "pytest-randomly")
    session.run("coverage", "run", "-m", "pytest")
    session.run("coverage", "report", "-m")
    session.run("coverage", "erase")


@nox.session(python="3.11", reuse_venv=True)
def isort(session):
    session.install("isort")
    session.run("isort", "--check", "--diff", "--python-version 311",
                "getfactormodels", "tests")

# TODO: mypy, ruff
