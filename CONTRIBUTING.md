# Contributing to Goliat

Thank you for your interest in contributing to Goliat! This document outlines the process for contributing to the project.

## Signed Commits Required

All contributions to Goliat **must** be submitted with signed commits. This is required to ensure that you have read and agree to the project's licensing terms, and that you have the right to contribute the code under those terms.

### How to Sign Commits

1. If you haven't already, configure Git with your GPG key:
   ```bash
   git config --global user.signingkey <your-gpg-key-id>
   git config --global commit.gpgsign true
   ```

2. When committing changes, use the `-S` flag:
   ```bash
   git commit -S -m "Your commit message"
   ```
   
   Or if you've set `commit.gpgsign true` globally, simply:
   ```bash
   git commit -m "Your commit message"
   ```

3. Verify your commit is signed before pushing:
   ```bash
   git log --show-signature
   ```

For more information about signing commits, see [GitHub's documentation on signing commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).

## Contributing Process

1. Read and agree to the Developer Certificate of Origin and Contributor License Agreement in `DCO.md`
2. Add your name to `CONTRIBUTORS.md` through a signed commit
3. Fork the repository
4. Create a feature branch
5. Make your changes
6. **Ensure all commits are signed**
7. Include a `Signed-off-by` line in commit messages
8. Write or update tests as needed
9. Submit a pull request

Your first pull request must include the signed commit adding your name to `CONTRIBUTORS.md`.

## Code Style

- Follow Go's official style guide and conventions
- Run `go fmt` before committing
- Write clear commit messages that explain the change

## Questions?

If you have questions about contributing, please open an issue in the repository.

## License Implications

By submitting a pull request with signed commits, you agree to two important terms:

1. Your contributions will be licensed under the same terms as the rest of the project (see the `LICENSE` file)
2. You grant the project owner (filcuc) the right to relicense your contributions under different terms in the future, including but not limited to different open source or proprietary licenses

This dual agreement is why we require signed commits - to ensure explicit agreement to both the current licensing terms and potential future relicensing of the code. Once a contribution is accepted, you cannot withdraw permission for its use or relicensing.