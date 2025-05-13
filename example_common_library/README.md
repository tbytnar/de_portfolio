# Common Library
This package contains methods and configuration values for use in the environments for interacting with various services such as Snowflake or AWS.

On a merged commit, Github actions will perform the following steps:
1 - Start a remote action runner in AWS EC2.
2 - Execute the version detection, build and publish the package to the private python repository.
3 - Shutdown and terminate the remote action runner in AWS EC2.

## External Links
This library makes use of Poetry for building and publishing:
https://python-poetry.org/

It also makes use of semantic-release for automated version bumping:
https://python-semantic-release.readthedocs.io/en/latest/


## Building
Python Semantic Release (PSR) uses Angular Style Commit parsing to determine the next version of the library.  https://github.com/angular/angular.js/blob/master/DEVELOPERS.md#commits

In order for this to work, your commit must begin with one of the following keywords:
- fix: A bug fix (Patch Version Bump: 0.0.0 -> 0.0.1)
- perf: A code change that improves performance (Patch Version Bump: 0.0.0 -> 0.0.1)
- feat: A new feature (Minor Version Bump: 0.0.0 -> 0.1.0)
- BREAKING CHANGE: A major code rewrite or breaking change (Major Verison Bump: 0.0.0 -> 1.0.0)


**NOTE**: The following keywords are also valid, however WILL NOT INCREMENT THE VERSION.  If used, the Github CI action will fail.  TODO: Tim you really need to fix this one day.
- docs: Documentation only changes
- chore: Changes to the build process or auxiliary tools and libraries such as documentation generation
- style: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- refactor: A code change that neither fixes a bug nor adds a feature
- test: Adding missing or correcting existing tests

If you want to preview what the version will be AFTER you have committed your code but BEFORE you have pushed it to the repository, execute the following: `semantic-release version --no-vcs-release --no-changelog --print`