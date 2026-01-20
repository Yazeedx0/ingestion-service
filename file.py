import httpx
import tempfile 
import os
import uuid
import hashlib
from app.core.openai_client import get_openai_client
import base64
from typing import List, Tuple
import json
import fitz
from pydantic import ValidationError
from app.service.ingestion import IngestionService
from app.models.legal_document import LegalDocument
from app.models.legal_chunk import LegalChunk  
from app.core.http_headers import BROWSER_HEADERS
from app.schemas.vlm import VLMChunk, VLMChunksOnlyResponse, VLMDocumentResponse



class FileService:

    def __init__(self):

        self.client = get_openai_client()
        self.ingestion_service = IngestionService()


    # Step 1: Download pdf 
    async def download_pdf(self, url: str) -> str:
        
        async with httpx.AsyncClient(timeout=40, follow_redirects=True) as client:
            response = await client.get(url, headers=BROWSER_HEADERS)
            response.raise_for_status()

            # Create temp file 
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                temp_path = tmp.name
                tmp.write(response.content)

        return temp_path
    
    async def parse_pdf_with_vlm(self, pdf_path: str) -> dict:

        try:
            # Convert PDF to images using PyMuPDF
            with fitz.open(pdf_path) as pdf_document:
                total_pages = len(pdf_document)
                all_images_base64 = []
                
                # Convert all pages to images
                for page_num in range(total_pages):
                    page = pdf_document[page_num]
                    # Render page to image at 150 DPI
                    pix = page.get_pixmap(matrix=fitz.Matrix(150/72, 150/72))
                    img_data = pix.tobytes("png")
                    
                    # Convert to base64
                    img_base64 = base64.standard_b64encode(img_data).decode("utf-8")
                    all_images_base64.append(img_base64)
            
            # Process pages in batches of 10
            batch_size = 10
            all_chunks = []
            document_title = None
            document_year = None
            document_ministry = None
            
            for batch_start in range(0, total_pages, batch_size):
                batch_end = min(batch_start + batch_size, total_pages)
                batch_images = all_images_base64[batch_start:batch_end]
                
                # First batch extracts title and year, subsequent batches only extract chunks
                if batch_start == 0:
                    prompt_text = (
                        "حلل هذا المستند القانوني، "
                        "واستخرج عنوان المستند وسنة الإصدار والوزارة أو الجهة المصدرة والنص من جميع الصفحات ثم قسمه إلى مقاطع منطقية. "
                        "أعد النتيجة بصيغة JSON فقط بدون أي نص إضافي كالتالي:\n"
                        '{"title": "عنوان المستند الكامل", "year": 2024, "ministry": "اسم الوزارة أو الجهة المصدرة", "chunks": [{"chunk_id": 1, "title": "عنوان القسم", "content": "محتوى القسم"}]}\n'
                        "تأكد من استخراج عنوان المستند وسنة الإصدار والوزارة أو الجهة المصدرة بدقة من الصفحة الأولى، واستخراج كل النص بدقة."
                    )
                else:
                    prompt_text = (
                        f"هذه الصفحات من {batch_start + 1} إلى {batch_end} من نفس المستند القانوني. "
                        "استخرج النص من جميع الصفحات وقسمه إلى مقاطع منطقية. "
                        "أعد النتيجة بصيغة JSON فقط بدون أي نص إضافي كالتالي:\n"
                        '{"chunks": [{"chunk_id": 1, "title": "عنوان القسم", "content": "محتوى القسم"}]}\n'
                        "تأكد من استخراج كل النص بدقة. استمر في ترقيم chunk_id من حيث توقفت."
                    )
                
                # Build the content array with text prompt and batch images
                content = [{"type": "text", "text": prompt_text}]
                
                # Add batch page images
                for img_base64 in batch_images:
                    content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{img_base64}",
                            "detail": "high"
                        }
                    })

                # Send to VLM for parsing using chat completions API
                response = await self.client.chat.completions.create(
                    model="gpt-4.1-mini",
                    messages=[{
                        "role": "user",
                        "content": content
                    }],
                    max_tokens=16000,
                    temperature=0.1
                )
                
                output_text = response.choices[0].message.content
                
                # Remove markdown code blocks if present
                if output_text.startswith("```"):
                    lines = output_text.split("\n")
                    output_text = "\n".join(lines[1:-1])
                    if output_text.startswith("json"):
                        output_text = output_text[4:]
                    output_text = output_text.strip()

                batch_data = json.loads(output_text)
                
                # Validate JSON response using Pydantic
                if batch_start == 0:
                    # First batch: validate with full schema (title, year, ministry, chunks)
                    try:
                        validated_data = VLMDocumentResponse.model_validate(batch_data)
                        document_title = validated_data.title
                        document_year = validated_data.year
                        document_ministry = validated_data.ministry
                        batch_chunks = [chunk.model_dump() for chunk in validated_data.chunks]
                    except ValidationError as e:
                        raise RuntimeError(f"Invalid VLM response format (first batch): {e}")
                else:
                    # Subsequent batches: validate chunks only
                    try:
                        validated_data = VLMChunksOnlyResponse.model_validate(batch_data)
                        batch_chunks = [chunk.model_dump() for chunk in validated_data.chunks]
                    except ValidationError as e:
                        raise RuntimeError(f"Invalid VLM response format (batch {batch_start // batch_size + 1}): {e}")
                
                # Re-number chunk_ids to ensure uniqueness across batches
                for chunk in batch_chunks:
                    chunk["chunk_id"] = len(all_chunks) + 1
                    all_chunks.append(chunk)

            return {
                "title": document_title,
                "year": document_year,
                "ministry": document_ministry,
                "chunks": all_chunks
            }
        
        except Exception as e:
            raise RuntimeError(f"Failed to parse PDF: {e}")

    def _transform_vlm_chunks_to_ingestion_format(self, vlm_chunks: List[dict]) -> List[dict]:

        chunks_data = []
        for chunk in vlm_chunks:
            chunk_id = chunk.get("chunk_id", "")
            title = chunk.get("title", "")
            content = chunk.get("content", "")
            
            # Combine title and content as text
            text = f"{title}\n{content}" if title else content
            
            chunks_data.append({
                "id": str(uuid.uuid4()),
                "article_number": str(chunk_id),
                "paragraph_number": None,
                "text": text.strip()
            })
        
        return chunks_data

    async def ingest_pdf_document(
        self,
        url: str,
    ) -> Tuple[LegalDocument, List[LegalChunk]]:

        pdf_path = None
        try:
            # Generate URL hash for duplicate detection
            url_hash = self._generate_url_hash(url)
            
            # Step 1: Download PDF
            pdf_path = await self.download_pdf(url)
            
            # Step 2: Parse PDF with VLM to extract metadata and chunks
            parsed_data = await self.parse_pdf_with_vlm(pdf_path)
            
            # Extract metadata from LLM response
            title = parsed_data.get("title", "Untitled Document")
            year = parsed_data.get("year", 2024)
            ministry = parsed_data.get("ministry", "وزارة العمل")
            vlm_chunks = parsed_data.get("chunks", [])
            
            # Generate unique document ID
            document_id = str(uuid.uuid4())
            
            # Step 3: Transform chunks to ingestion format
            chunks_data = self._transform_vlm_chunks_to_ingestion_format(vlm_chunks)
            
            # Step 4: Ingest document with chunks and embeddings
            document, chunks = await self.ingestion_service.ingest_document_with_chunks(
                document_id=document_id,
                title=title,
                year=year,
                chunks_data=chunks_data,
                source_url=url,
                url_hash=url_hash,
                ministry=ministry
            )
            
            return document, chunks
            
        finally:
            # Step 5: Clean up temporary file
            if pdf_path:
                self._cleanup_temp_file(pdf_path)

    def _generate_url_hash(self, url: str) -> str:
        """Generate SHA-256 hash of URL for duplicate detection."""
        # Normalize URL by stripping whitespace and converting to lowercase
        normalized_url = url.strip().lower()
        return hashlib.sha256(normalized_url.encode('utf-8')).hexdigest()

    def _cleanup_temp_file(self, file_path: str) -> None:

        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            # Log but don't raise - cleanup failure shouldn't break the workflow
            print(f"Warning: Failed to cleanup temp file {file_path}: {e}") 
