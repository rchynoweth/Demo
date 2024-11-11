# Databricks notebook source
# MAGIC %md
# MAGIC # Object Character Recognition on Databricks - IN PROGRESS
# MAGIC
# MAGIC
# MAGIC We are going to use the Databricks Labs [project]() as an example. The base requirement is to install the following maven dependency on the cluster: `com.databricks.labs:tika-ocr:0.1.4`.  
# MAGIC
# MAGIC
# MAGIC
# MAGIC This notebook shows an end to end workflow that extracts text from images and uses an LLM to summarize the results so that they can be structured and analyzed. 
# MAGIC
# MAGIC This notebook uses the following image to text [model](https://huggingface.co/stepfun-ai/GOT-OCR2_0), Llama 3.1 __ language model, and runs on DBR ML 15LTS with GPUs. 
# MAGIC
# MAGIC
# MAGIC https://www.jaided.ai/easyocr/
# MAGIC
# MAGIC
# MAGIC https://github.com/Ucas-HaoranWei/GOT-OCR2.0/blob/main/GOT-OCR-2.0-master/GOT/demo/run_ocr_2.0.py
# MAGIC
# MAGIC https://github.com/Ucas-HaoranWei/GOT-OCR2.0/blob/main/GOT-OCR-2.0-master/GOT/model/plug/blip_process.py
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install verovio

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# set and get notebook parameters 
dbutils.widgets.text('catalog_name', '')
dbutils.widgets.text('schema_name', '')
dbutils.widgets.text('volume_name', '')

catalog_name = dbutils.widgets.get('catalog_name')
schema_name = dbutils.widgets.get('schema_name')
volume_name = dbutils.widgets.get('volume_name')

print(f"{catalog_name} | {schema_name} | {volume_name}")

# COMMAND ----------

from transformers import AutoTokenizer, AutoModelForCausalLM, AutoModel
import requests
from PIL import Image, ImageEnhance
from io import BytesIO
import torch

# COMMAND ----------

model_name = 'stepfun-ai/GOT-OCR2_0'
image_path = "/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg"  


# COMMAND ----------

tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
model = AutoModel.from_pretrained(model_name, low_cpu_mem_usage=True, device_map='cuda', use_safetensors=True, pad_token_id=151643, trust_remote_code=True).eval()
model.to(device='cuda',  dtype=torch.bfloat16)

# COMMAND ----------

def load_image(image_file):
    if image_file.startswith('http') or image_file.startswith('https'):
        response = requests.get(image_file)
        image = Image.open(BytesIO(response.content)).convert('RGB')
    else:
        image = Image.open(image_file).convert('RGB')
    return image

# COMMAND ----------


from torchvision import transforms
from torchvision.transforms.functional import InterpolationMode



def process_image(item, image_size=1024, mean=None, std=None):
    if mean is None:
      mean = (0.48145466, 0.4578275, 0.40821073)
    if std is None:
      std = (0.26862954, 0.26130258, 0.27577711)
    
    normalize = transforms.Normalize(mean, std)
    transform = transforms.Compose(
            [
                transforms.Resize(
                    (image_size, image_size), interpolation=InterpolationMode.BICUBIC
                ),
                transforms.ToTensor(),
                normalize,
            ]
        )
    return transform(item)


# COMMAND ----------

image = load_image(image_path)
image_copy = image.copy()
image_tensor_1 = process_image(image)
image_tensor_2 = process_image(image_copy)

# COMMAND ----------

prompt = "I am going to give you the output of an OCR AI model. I would like you to take the output and provide key value pairs of various data points to be consumed via JSON."
inputs = tokenizer([prompt])
input_ids = torch.as_tensor(inputs.input_ids).cuda()

# COMMAND ----------

input_ids.shape

# COMMAND ----------

with torch.autocast("cuda", dtype=torch.bfloat16):
  output_ids = model.generate(
      input_ids,
      images=[image_tensor_1.unsqueeze(0).cuda()],
      do_sample=False,
      num_beams = 1,
      no_repeat_ngram_size = 20,
      # streamer=streamer,
      max_new_tokens=4096,
      # stopping_criteria=[stopping_criteria]
      )
  
output_ids

# COMMAND ----------

outputs = tokenizer.decode(output_ids[0, input_ids.shape[1]:]).strip()
print(outputs)

# COMMAND ----------


