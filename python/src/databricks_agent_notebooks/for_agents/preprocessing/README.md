# Preprocessing

Notebook source files pass through a text-based preprocessing step before
notebook parsing. This step expands directives written with `{! !}` syntax.

Preprocessing applies to all text-based notebook formats: `.md`, `.py`, `.scala`,
and `.sql`. Only `.ipynb` files are excluded (they are JSON, not plain text).

## How it works

```
raw file text --> preprocess_text() --> preprocessed text --> to_notebook() --> pipeline
```

Preprocessing runs on every `agent-notebook run` by default. If the source
text contains no `{!` marker, it short-circuits immediately with zero
overhead -- no Jinja2 environment is created.

## Directive syntax

```
{! plugin_name("argument") !}
```

Directives are Jinja2 expressions using custom delimiters. The `{! !}`
delimiters were chosen to avoid conflicts with Python f-strings, standard
Jinja2/dbt templates (`{{ }}`), LaTeX, bash, SQL, and markdown.

Standard Jinja2 delimiters (`{{ }}`, `{% %}`, `{# #}`) in notebook content
are left untouched.

## Built-in plugins

See `plugins.md` for the full reference.

- **`include`** -- inline file content by relative or absolute path

Example:

```markdown
# My Notebook

```python
{! include("shared/spark_setup.py") !}
```

```python
{! include("../../python/src/shared/helpers.py") !}
```

```python
df = spark.range(10)
df.show()
```
```

## Built-in filters and globals

The preprocessing environment provides filters (`fromjson`, `split`,
`basename`, `dirname`, `regex_search`, `regex_replace`) and globals
(`env`, `now`) from the Python standard library.

See the `best_practices` section in `plugins.md` for the full reference
with examples and links to Python documentation.

## Opting out

- `--no-preprocess` flag skips the preprocessing step entirely

## Limitations

- Included content is inserted verbatim -- directives inside included files
  are not expanded (no recursive processing).
- Directives cannot access notebook frontmatter or runtime state. Plugin
  context is limited to the notebook file path.
- Unrecognized directive names produce a clear error
  (`PreprocessorError: Unknown directive`), not silent pass-through.
