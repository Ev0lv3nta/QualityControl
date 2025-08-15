-- MIGRATION: переход на новую версию без потери данных
BEGIN;

-- 1) Функция last_activity (создать/обновить)
CREATE OR REPLACE FUNCTION public.update_user_last_activity_func()
RETURNS TRIGGER AS $body$
BEGIN
  UPDATE public.users SET last_activity = NOW() WHERE user_id = NEW.user_id;
  RETURN NEW;
END;
$body$ LANGUAGE plpgsql;

-- 2) Триггер на control_data (создать, если нет)
DO $do$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    WHERE t.tgname = 'trigger_user_activity_on_control_data'
      AND c.relname = 'control_data'
  ) THEN
    EXECUTE '
      CREATE TRIGGER trigger_user_activity_on_control_data
      AFTER INSERT OR UPDATE ON public.control_data
      FOR EACH ROW
      EXECUTE FUNCTION public.update_user_last_activity_func()
    ';
  END IF;
END
$do$;

-- 3) Индексы (создать при отсутствии)
CREATE INDEX IF NOT EXISTS idx_control_data_stage            ON public.control_data(stage_name);
CREATE INDEX IF NOT EXISTS idx_control_data_user             ON public.control_data(user_id);
CREATE INDEX IF NOT EXISTS idx_control_data_created_desc     ON public.control_data(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_control_data_value_numeric    ON public.control_data(value_numeric) WHERE value_numeric IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_control_data_forming_session  ON public.control_data(forming_session_id) WHERE forming_session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_control_data_data_gin         ON public.control_data USING GIN (data);

CREATE INDEX IF NOT EXISTS idx_forming_sessions_qr           ON public.forming_sessions(frame_qr_text);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_forming_session
  ON public.forming_sessions(frame_qr_text)
  WHERE completed_at IS NULL;

-- 4) Добавляем хранение кода тары в формовке
ALTER TABLE public.forming_sessions
    ADD COLUMN IF NOT EXISTS frame_qr_tare text;

-- 5) Бэкофл данных ЦГП: дублируем старое поле в новое и наоборот
UPDATE public.control_data
SET data = jsonb_set(data, '{cgp_qr_goods}', to_jsonb(data->>'cgp_qr_text'), true)
WHERE stage_name='cgp'
  AND NOT (data ? 'cgp_qr_goods')
  AND data ? 'cgp_qr_text';

UPDATE public.control_data
SET data = jsonb_set(data, '{cgp_qr_text}', to_jsonb(data->>'cgp_qr_goods'), true)
WHERE stage_name='cgp'
  AND NOT (data ? 'cgp_qr_text')
  AND data ? 'cgp_qr_goods';

COMMIT;
