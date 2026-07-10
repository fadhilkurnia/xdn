# papers

This directory is the designated home for XDN paper projects, but it is
intentionally (almost) empty in this public repository.

Each paper lives in its **own private GitHub repository** because papers are
submitted under double-blind review and must not be linkable to the authors.
The public code repo therefore ignores everything under `papers/` except this
README (see the `papers/*` / `!papers/README.md` rules in the top-level
`.gitignore`).

To work on a paper, clone its private repo into this folder, for example:

```
git clone git@github.com:<owner>/<paper>-paper.git papers/<paper>-paper
```

Its contents stay untracked here and cannot leak into this public repo.
