#!/usr/bin/env python

import argparse
import os
import zipfile
from tqdm import tqdm
import time
# from mosestokenizer import *
from sacremoses import MosesTokenizer, MosesDetokenizer
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from functools import partial
import json
from multiprocessing import Pool, RLock, shared_memory
from multiprocessing.managers import SharedMemoryManager

def parse(content_xml):
  root = ET.fromstring(content_xml)
  sentences = []
  for sentence in root.iter('s'):
    sentences.append([token.text for token in sentence.iter(tag='w')])
  return sentences


def untokenize(tokenized_sentences, language):
  md = MosesDetokenizer(lang=language)
  untokenized_sentences = [md.detokenize(ts) for ts in tokenized_sentences]


def preprocess(language, content_xml_zf):
  content_xml, zf = content_xml_zf
  try:
    content_xml = "".join([b.decode('utf8', 'ignore') for b in content_xml])
    sentences = parse(content_xml)
    sentences = untokenize(sentences, language)
    sentences = '\n'.join(sentences)
    # sentences = re.sub('\\n', '\\\\n', sentences)
    if len(sentences):
        entry = {
          'subtitles': sentences,
          'language': language,
          'year': zf.split('/')[3],
          'IMDbs': zf.split('/')[4],
          'filename': zf.split('/')[5]
        }
        return entry
  except:
    print('could not parse file {}.'.format(zf))
    return
  

def preprocess_chunk(language, n, n_max, content_xml_zf, flag):
    preprocess_language = partial(preprocess, language)
    interval = 0.001 / (n * (9 - n) + 2)
    total = len(content_xml_zf)
    entries = []

    # text = "#{},".format(n)
    text = "[#" + "{}".format(str(n)).zfill(4) + "] Processing queue for language: {}".format(language)
    bar = tqdm(total=total, desc=text, position=n+1, leave=False)
    for q in content_xml_zf:
        e = preprocess_language(q)
        # e = preprocess(q)
        if e is not None:
          entries.append(e)
        bar.update(1)

    bar.refresh()
    start_t = time.time()

    lock = tqdm.get_lock()
    if n == n_max - 1:
        try:
            lock.acquire()
            flag[n] = 1
            bar.start_t += time.time() - start_t # to correct the elapsed time caused by lock acquirement and  while-loop checking.
            bar.close()
        except Exception as err:
            raise err
        finally:
            lock.release()
            return entries
    else:
        while 1:
            try:
                lock.acquire()
                next_flag = flag[n + 1]

                if next_flag == 1:
                    flag[n] = 1
                    bar.start_t += time.time() - start_t
                    bar.close()
                    return

            except Exception as err:
                raise err
            finally:
                lock.release()
                return entries

  
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def process_opus(language, input_dir, output_dir, n_procs, queue_len):
  """language should be in 'fr', 'en', 'zh_cn', 'pt', 'es', 'ar'"""
  if n_procs > 1:
    preprocess_chunk_language = partial(preprocess_chunk, language)
  else:
    preprocess_language = partial(preprocess, language)
  with zipfile.ZipFile(os.path.join(input_dir, '{}.zip'.format(language)), 'r') as zip_:
    with open(os.path.join(output_dir, '{}.jsonl'.format(language)), 'w') as outfile:
      file_list = [f for f in zip_.namelist() if f.endswith('.xml')] # List .gz files
      nb_files = len(file_list)
      print("Found {:,} files for language {}.".format(nb_files, language))

      # extract a specific file from the zip container
      queue = []
      for zf in tqdm(file_list, desc='Reading files for language: {}'.format(language), leave=True):
        if zf.endswith('.xml'):
          with zip_.open(zf, 'r') as f:
            content_xml = f.readlines()
          queue.append((content_xml, str(zf)))
          if len(queue) >= queue_len:
            queue_chunks = list(chunks(queue, queue_len // n_procs))
            L = list(range(len(queue_chunks)))
            L_max = [n_procs for _ in range(len(queue_chunks))]
            if n_procs > 1:
              with SharedMemoryManager() as smm:
                finished_flags = smm.ShareableList([0] * n_procs)
                flags = [finished_flags] * n_procs
                lock = tqdm.get_lock()
                with Pool(processes=n_procs, initializer=tqdm.set_lock, initargs=(lock,)) as pool:
                  entries = pool.starmap(preprocess_chunk_language, reversed(list(zip(L, L_max, queue_chunks, flags))))
                entries = [item for sublist in entries for item in sublist]  # flatten
            else:
              entries = []
              for q in tqdm(queue, desc='Processing queue for language: {}'.format(language), leave=False):
                e = preprocess_language(q)
                entries.append(e)
            for e in entries:
              if e is not None:
                json.dump(e, outfile)
                outfile.write('\n')
            queue = []
            
            
if __name__ == "__main__":
  
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--input_dir', help="input directory for subtitle file")
  parser.add_argument('-o', '--output_dir', help="output directory for jsonl file")
  parser.add_argument('-q', '--queue_len', help="queue length to avoid continuous i/o on disk", type=int)
  parser.add_argument('-p', '--n_procs', help="number of processes to parallelize process", type=int)
  parser.add_argument('-l', '--language', help="language file")

  args = parser.parse_args()
  
  process_opus(args.language, args.input_dir, args.output_dir, args.n_procs, args.queue_len)
  
  # python preprocess_open_subtitles.py --input_dir /data/asimouli/opensubtitles/ --output_dir /data/asimouli/opensubtitles/ --queue_len 10000 --n_procs 5 --language es

  
  
