# opus-open-subtitles-monolingual

Contain scripts to preprocess the Open Subtitle mono lingual corpus.
To avoid to many I/O on disk, subtitles documents are handled in queues. You first load `queue_len` document in memory. You can then process them (parse XML and de-tokenize) using multiprocessing.
You can set the number of workers with `n_procs`. 

You can download files from the [opus website](https://opus.nlpl.eu/OpenSubtitles-v2018.php):

```bash
wget https://opus.nlpl.eu/download.php?f=OpenSubtitles/v2018/xml/fr.zip -O fr.zip
```

You can then preprocess the file into `jsonl` format with one line per subtitle:

```bash
python preprocess.py --input_dir /data/.. --output_dir /data/... --queue_len 10000 --n_procs 5 --language fr
```

Finally you ay compress the file to optimize disk space:

```bash
gzip -c fr.jsonl > fr.jsonl.gz
```

You may then read the dataset with HuggingFace `datasets`library:

```python
from datasets import load_dataset

dataset = load_dataset(
  'json', 
  data_files='{}.jsonl'.format("fr"))
```
