# Future Work

## Primary (for agents)

- SKILL
- Re-design doctor output for agents -> to go into memory
- include cell termination signals in run output to allow agents to do follow-up tasks in parallel
- support for %run magic command (local-side includes)
- streaming nbconvert output
- use standard nbformat git coordinates once [our PR](https://github.com/jupyter/nbformat/pull/427) is accepted

## Secondary (for humans)

- support SQL cells in Python & Scala notebooks (rewrite as `display(spark.sql("...")))`)
