# Reproducibility and Data Version Control for LangChain and LLM/OpenAI Models

Start by ⭐️ starring [lakeFS open source](https://go.lakefs.io/oreilly-course) project.

This repository includes a Jupyter Notebook with LangChain and OpenAI libraries which you can run on your local machine.

## Let's Get Started 👩🏻‍💻

Clone this repository

   ```bash
   git clone https://github.com/treeverse/lakeFS-samples && cd lakeFS-samples/01_standalone_examples/llm-openai-langchain-integration
   ```

You now have two options: 

### **Run a Notebook server with your existing lakeFS Server**

If you have already [installed lakeFS](https://docs.lakefs.io/deploy/) or are utilizing [lakeFS cloud](https://lakefs.cloud/), all you need to run is the Jupyter notebook with LangChain and OpenAI libraries (Docker image size will be around 10GB):


   ```bash
   docker compose up 
   ```

### **Don't have a lakeFS Server or Object Store?**

If you want to provision a lakeFS server as well as MinIO for your object store, plus Jupyter with LangChain and OpenAI libraries then bring up the full stack:

   ```bash
   docker compose --profile local-lakefs up
   ```

### URLs and login details

* Jupyter http://localhost:8891/

If you've brought up the full stack you'll also have: 

* LakeFS http://localhost:48000/ (`AKIAIOSFOLKFSSAMPLES` / `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`)
* MinIO http://localhost:49001/ (`minioadmin`/`minioadmin`)


## Demo Instructions

Open Jupyter UI [http://localhost:8891](http://localhost:8891) in your web browser. Open "LLM OpenAI LangChain Demo" notebook from Jupyter UI and follow the instructions.