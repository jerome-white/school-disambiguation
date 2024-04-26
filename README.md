# School Name Disambiguation

## Get data

Assuming you have the following Bash enviroment setup:

```bash
$> env | grep 'GOOGLE_API_KEY'
GOOGLE_API_KEY=...
$> G_SHEET=...
```

Run the following:

```
$> python get-data.py --sheet-id $G_SHEET | python clean-targets.py
```

Output goes to stdout, so redirect accordingly.

## Modelling

Ensure your Python environment is adequate:

```
$> python -m venv
$> source venv/bin/activate
$> pip install --requirement requirements.txt
```

### Link Transformers

Assuming data was written to `student-responses.csv`:

```bash
$> python models/link-transformer.py < student-responses.csv
```
