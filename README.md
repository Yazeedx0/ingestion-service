# Ingestion-service

---

## Overview

This service is designed to extract structured data from **official legal PDF documents**, especially those that are **unreadable or scanned** (image-based PDFs), such as files published on government and ministry websites.

Many official documents cannot be processed using traditional text extraction or OCR tools. This service solves that problem by using **visual-based parsing** and a robust ingestion pipeline.
![Uploading image.png…]()

---

## Problem

* Government PDFs are often:

  * Scanned images
  * Poorly formatted
  * Not machine-readable
* Traditional OCR is unreliable and error-prone
* Legal documents require **high accuracy and structure**

---

## Solution

The service converts PDFs into images and uses a **Vision-Language Model (VLM)** to extract:

* Document title
* Year of issuance
* Issuing ministry or authority
* Full content, split into logical sections (chunks)

The extracted data is then validated, normalized, and stored in a searchable format.

---

## High-Level Flow

1. Receive a PDF URL via API
2. Process the request asynchronously using **Celery**
3. Download the PDF temporarily
4. Convert PDF pages into images
5. Parse content using a Vision-Language Model
6. Validate extracted data using strict schemas
7. Normalize and transform the content
8. Generate a URL hash to prevent duplicates
9. Store:

   * Document metadata in a document store
   * Content embeddings in a vector store
10. Clean up temporary files

---

## Key Features

* Handles unreadable and scanned PDFs
* Asynchronous processing with Celery
* Strict schema validation (fail fast on invalid data)
* Logical content chunking for search and retrieval
* Duplicate detection using URL hashing
* Clean separation between parsing, validation, and storage

---

## Use Cases

* Legal document ingestion
* Government data processing
* Semantic search over official documents
* Legal AI and question-answering systems

---

## Design Principles

* Accuracy over speed
* Fail fast on invalid data
* AI as a parser, not a source of truth
* Clear execution boundaries
* Clean and maintainable architecture

---

## Notes

This service focuses on **engineering reliability around AI**, ensuring that extracted data is structured, validated, and safe to use in production systems.

---
