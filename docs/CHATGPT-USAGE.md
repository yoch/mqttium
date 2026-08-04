# GitHub workflow for ChatGPT

For multi-file modifications, use one atomic Git commit.

1. Read the target PR and record its exact head SHA and head branch.
2. Fetch the complete current content of every file that will be modified.
3. Create one blob for each resulting file.
4. Create one tree based on the current commit tree.
5. Create one commit whose sole parent is the recorded head SHA.
6. Inspect the candidate commit diff before publishing it.
7. Confirm that:
   - only intended files changed;
   - no unrelated content was removed;
   - no temporary file was added;
   - the diff matches the requested work.
8. Move the PR branch to the candidate commit using `force: false`.
9. Read the PR again and verify that its head SHA is the new commit.
10. Check CI for the pushed commit.

Never:
- use sequential `create_file` or `update_file` calls for a multi-file change;
- force-push;
- update the branch without inspecting the candidate commit;
- reuse an outdated head SHA;
- modify the default branch directly.

For a single very small file, `update_file` is acceptable.