# Databricks notebook source
# MAGIC %md
# MAGIC # Object Character Recognition on Databricks - IN PROGRESS
# MAGIC
# MAGIC
# MAGIC This notebook shows an end to end workflow that extracts text from images and uses an LLM to summarize the results so that they can be structured and analyzed. 
# MAGIC
# MAGIC This notebook uses the following image to text [model](https://huggingface.co/stepfun-ai/GOT-OCR2_0), Llama 3.1 __ language model, and runs on DBR ML 15LTS with GPUs. 
# MAGIC
# MAGIC
# MAGIC https://www.jaided.ai/easyocr/

# COMMAND ----------



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

from transformers import TrOCRProcessor, VisionEncoderDecoderModel
import requests
from PIL import Image, ImageEnhance

# COMMAND ----------

model_name = 'microsoft/trocr-base-printed'
image_path = "/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg"  # Replace with your image path


# COMMAND ----------

processor = TrOCRProcessor.from_pretrained(model_name)
model = VisionEncoderDecoderModel.from_pretrained(model_name)

# load image
image = Image.open(image_path).convert("RGB")

# Enhance contrast (optional, helpful if text is faint)
enhancer = ImageEnhance.Contrast(image)
image = enhancer.enhance(2)

pixel_values = processor(image, return_tensors="pt").pixel_values
generated_ids = model.generate(pixel_values)

generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True, max_new_tokens=300)[0]
print(generated_text)

# COMMAND ----------

from transformers import TrOCRProcessor, VisionEncoderDecoderModel
from PIL import Image, ImageEnhance
import requests

# Load the processor and model for printed text
processor = TrOCRProcessor.from_pretrained("microsoft/trocr-base-printed")
model = VisionEncoderDecoderModel.from_pretrained("microsoft/trocr-base-printed")

# Function to preprocess grocery product images
def preprocess_image(image_path):
    # Load image
    image = Image.open(image_path)
    
    # Enhance contrast (optional, helpful if text is faint)
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2)
    
    # Resize image if needed (maintains aspect ratio)
    image = image.resize((1024, 1024), Image.ANTIALIAS)
    
    return image

# Load and preprocess the image
image_path = "/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg"  # Replace with your image path
image = preprocess_image(image_path)

# Preprocess and generate text
pixel_values = processor(images=image, return_tensors="pt").pixel_values
generated_ids = model.generate(pixel_values)

# Decode to text
generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)
print("Recognized Text:", generated_text)


# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install spark-nlp

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from sparkocr.transformers import BinaryToImage


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from transformers import TrOCRProcessor, VisionEncoderDecoderModel
import requests
from PIL import Image

model_name = "microsoft/trocr-large-printed"
processor = TrOCRProcessor.from_pretrained(model_name)
model = VisionEncoderDecoderModel.from_pretrained(model_name)

# load image from the IAM dataset
# url = "https://fki.tic.heia-fr.ch/static/img/a01-122-02.jpg"
# image = Image.open(requests.get(url, stream=True).raw).convert("RGB")

image = Image.open("/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg")#.convert("RGB")

pixel_values = processor(image, return_tensors="pt").pixel_values
generated_ids = model.generate(pixel_values)

generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
generated_text

# COMMAND ----------

import requests

import torch
from PIL import Image
from transformers import AutoProcessor, AutoModelForCausalLM 

device = "cuda:0" if torch.cuda.is_available() else "cpu"
torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32

model = AutoModelForCausalLM.from_pretrained("microsoft/Florence-2-large", torch_dtype=torch_dtype, trust_remote_code=True).to(device)
processor = AutoProcessor.from_pretrained("microsoft/Florence-2-large", trust_remote_code=True)

image = Image.open("/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg").convert("RGB")


def run_example(task_prompt, text_input=None):
    if text_input is None:
        prompt = task_prompt
    else:
        prompt = task_prompt + text_input
    inputs = processor(text=prompt, images=image, return_tensors="pt").to(device, torch_dtype)
    generated_ids = model.generate(
      input_ids=inputs["input_ids"],
      pixel_values=inputs["pixel_values"],
      max_new_tokens=1024,
      num_beams=3
    )
    generated_text = processor.batch_decode(generated_ids, skip_special_tokens=False)[0]

    parsed_answer = processor.post_process_generation(generated_text, task=task_prompt, image_size=(image.width, image.height))

    print(parsed_answer)



# COMMAND ----------

generated_text

# COMMAND ----------

from transformers import AutoModel, AutoTokenizer

# COMMAND ----------

tokenizer = AutoTokenizer.from_pretrained('ucaslcl/GOT-OCR2_0', trust_remote_code=True, padding='max_length')
model = AutoModel.from_pretrained('ucaslcl/GOT-OCR2_0', trust_remote_code=True, low_cpu_mem_usage=True, use_safetensors=True, pad_token_id=tokenizer.eos_token_id)

# COMMAND ----------

model = model.eval().cuda()

# COMMAND ----------

image_file = "/Volumes/rac_demo_catalog/rac_demo_db/rac_volume/sour_patch_20241104_081443.jpg"

# COMMAND ----------

# res = model.forward(tokenizer, image_file, ocr_type='ocr')
res = model.chat(tokenizer, image_file, ocr_type='format')

# COMMAND ----------

print(res)

# COMMAND ----------

import os

# Set the environment variable for the current notebook session
os.environ['PYTHONIOENCODING'] = 'utf-8'


# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import easyocr

reader = easyocr.Reader(['en'], gpu=True)

result = reader.readtext(image_file)


# COMMAND ----------

result

# COMMAND ----------

reader.readtext(image_file)
