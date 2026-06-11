-- Phase 18: monitor redirect handling and optional display URLs
-- NOTE: Keep this file append-only.

ALTER TABLE monitors
  ADD COLUMN follow_redirects INTEGER NOT NULL DEFAULT 1
  CHECK (follow_redirects IN (0, 1));

ALTER TABLE monitors
  ADD COLUMN display_url TEXT;
