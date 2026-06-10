-- Multi-HBL support: explicit upload sequence per txn_id + file_type
-- Run against the same database configured in .env (DB_NAME)

ALTER TABLE public.pdf_uploads
ADD COLUMN IF NOT EXISTS file_sequence INTEGER NOT NULL DEFAULT 1;

COMMENT ON COLUMN public.pdf_uploads.file_sequence IS
  'Upload order within txn_id and file_type. HBL: 1,2,3... MBL: 1';

-- Backfill existing rows: order by uploaded_at within each txn + file_type
WITH ranked AS (
    SELECT txn_id,
           filename,
           file_type,
           ROW_NUMBER() OVER (
               PARTITION BY txn_id, file_type
               ORDER BY uploaded_at, filename
           ) AS rn
    FROM public.pdf_uploads
)
UPDATE public.pdf_uploads AS p
SET file_sequence = ranked.rn
FROM ranked
WHERE p.txn_id = ranked.txn_id
  AND p.filename = ranked.filename
  AND p.file_type = ranked.file_type;

-- Prevent duplicate sequence slots per transaction and file type
CREATE UNIQUE INDEX IF NOT EXISTS uq_pdf_uploads_txn_type_sequence
ON public.pdf_uploads (txn_id, file_type, file_sequence);
