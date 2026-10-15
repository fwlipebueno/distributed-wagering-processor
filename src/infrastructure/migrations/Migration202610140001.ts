import { Migration } from "@mikro-orm/migrations";

export class Migration202610140001 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY, player_id uuid NOT NULL, currency text NOT NULL CHECK(currency IN ('BRL','USD','EUR')),
        balance numeric(20,2) NOT NULL CHECK(balance >= 0), version integer NOT NULL CHECK(version >= 1),
        created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
        UNIQUE(player_id, currency)
      );
      CREATE TABLE wagers (
        id uuid PRIMARY KEY, provider_id text NOT NULL, external_transaction_id text NOT NULL,
        idempotency_key text NOT NULL UNIQUE, payload_hash text NOT NULL CHECK(length(payload_hash)=64),
        wallet_id uuid NOT NULL REFERENCES wallets(id), player_id uuid NOT NULL,
        round_id text NOT NULL, game_id text NOT NULL,
        kind text NOT NULL CHECK(kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(20,2) NOT NULL CHECK(amount >= 0), currency text NOT NULL CHECK(currency IN ('BRL','USD','EUR')),
        reference_external_transaction_id text, reference_transaction_id uuid REFERENCES wagers(id),
        status text NOT NULL CHECK(status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text, created_at timestamptz NOT NULL, processed_at timestamptz, result jsonb,
        attempts integer NOT NULL DEFAULT 0 CHECK(attempts >= 0), next_attempt_at timestamptz,
        correlation_id text NOT NULL, causation_id text,
        UNIQUE(provider_id, external_transaction_id),
        CHECK(kind = 'LOSS' OR amount > 0),
        CHECK(kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CHECK(kind NOT IN ('BET','LOSS','OPENING') OR reference_external_transaction_id IS NULL),
        CHECK((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CHECK(status NOT IN ('PROCESSED','REJECTED','FAILED') OR (processed_at IS NOT NULL AND result IS NOT NULL)),
        CHECK(kind <> 'OPENING' OR provider_id = 'internal')
      );
      CREATE UNIQUE INDEX one_reversal_per_kind ON wagers(reference_transaction_id, kind)
        WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK');
      CREATE INDEX pending_references_due ON wagers(next_attempt_at, id) WHERE status='PENDING_REFERENCE';
      CREATE TABLE ledger (
        id uuid PRIMARY KEY, position bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
        wallet_id uuid NOT NULL REFERENCES wallets(id), transaction_id uuid NOT NULL REFERENCES wagers(id),
        direction text NOT NULL CHECK(direction IN ('DEBIT','CREDIT')),
        amount numeric(20,2) NOT NULL CHECK(amount > 0), currency text NOT NULL,
        balance_before numeric(20,2) NOT NULL CHECK(balance_before >= 0),
        balance_after numeric(20,2) NOT NULL CHECK(balance_after >= 0), created_at timestamptz NOT NULL,
        UNIQUE(wallet_id, transaction_id),
        CHECK(balance_after = balance_before + CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
      );
      CREATE INDEX ledger_wallet_position ON ledger(wallet_id, position);
      CREATE TABLE inbox (
        consumer_name text NOT NULL, message_id text NOT NULL, payload_hash text NOT NULL,
        received_at timestamptz NOT NULL, processed_at timestamptz,
        transaction_id uuid REFERENCES wagers(id), PRIMARY KEY(consumer_name,message_id),
        CHECK((processed_at IS NULL) = (transaction_id IS NULL))
      );
      CREATE TABLE outbox (
        id uuid PRIMARY KEY, aggregate_id uuid NOT NULL REFERENCES wallets(id), event_type text NOT NULL,
        payload jsonb NOT NULL, occurred_at timestamptz NOT NULL,
        attempts integer NOT NULL DEFAULT 0 CHECK(attempts >= 0), next_attempt_at timestamptz, published_at timestamptz
      );
      CREATE INDEX outbox_due ON outbox(next_attempt_at, occurred_at, id) WHERE published_at IS NULL;

      CREATE FUNCTION deny_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'immutable financial record' USING ERRCODE='23514'; END $$;
      CREATE TRIGGER immutable_ledger BEFORE UPDATE OR DELETE ON ledger FOR EACH ROW EXECUTE FUNCTION deny_mutation();
      CREATE TRIGGER no_ledger_truncate BEFORE TRUNCATE ON ledger FOR EACH STATEMENT EXECUTE FUNCTION deny_mutation();
      CREATE TRIGGER no_wager_delete BEFORE DELETE ON wagers FOR EACH ROW EXECUTE FUNCTION deny_mutation();
      CREATE TRIGGER no_wallet_delete BEFORE DELETE ON wallets FOR EACH ROW EXECUTE FUNCTION deny_mutation();

      CREATE FUNCTION guard_wager_transition() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'terminal wager is immutable' USING ERRCODE='23514';
        END IF;
        IF ROW(NEW.id,NEW.provider_id,NEW.external_transaction_id,NEW.idempotency_key,NEW.payload_hash,
          NEW.wallet_id,NEW.player_id,NEW.round_id,NEW.game_id,NEW.kind,NEW.amount,NEW.currency,
          NEW.reference_external_transaction_id,NEW.created_at,NEW.correlation_id,NEW.causation_id)
          IS DISTINCT FROM ROW(OLD.id,OLD.provider_id,OLD.external_transaction_id,OLD.idempotency_key,OLD.payload_hash,
          OLD.wallet_id,OLD.player_id,OLD.round_id,OLD.game_id,OLD.kind,OLD.amount,OLD.currency,
          OLD.reference_external_transaction_id,OLD.created_at,OLD.correlation_id,OLD.causation_id)
          OR (OLD.status='PENDING_REFERENCE' AND NEW.status='PENDING') THEN
          RAISE EXCEPTION 'invalid wager transition' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER wager_transition BEFORE UPDATE ON wagers FOR EACH ROW EXECUTE FUNCTION guard_wager_transition();

      CREATE FUNCTION guard_ledger_insert() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE previous_balance numeric; t wagers; w wallets; expected_direction text;
      BEGIN
        SELECT * INTO STRICT w FROM wallets WHERE id=NEW.wallet_id FOR UPDATE;
        SELECT * INTO STRICT t FROM wagers WHERE id=NEW.transaction_id;
        SELECT balance_after INTO previous_balance FROM ledger WHERE wallet_id=NEW.wallet_id ORDER BY position DESC LIMIT 1;
        expected_direction := CASE t.kind WHEN 'BET' THEN 'DEBIT' WHEN 'ROLLBACK' THEN
          (SELECT CASE kind WHEN 'BET' THEN 'CREDIT' ELSE 'DEBIT' END FROM wagers WHERE id=t.reference_transaction_id)
          ELSE 'CREDIT' END;
        IF NEW.balance_before <> COALESCE(previous_balance,0) OR t.status <> 'PROCESSED' OR t.kind='LOSS'
          OR t.wallet_id<>NEW.wallet_id OR t.player_id<>w.player_id OR t.amount<>NEW.amount
          OR t.currency<>NEW.currency OR w.currency<>NEW.currency OR NEW.direction<>expected_direction THEN
          RAISE EXCEPTION 'invalid ledger link or chain' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER ledger_insert BEFORE INSERT ON ledger FOR EACH ROW EXECUTE FUNCTION guard_ledger_insert();

      CREATE FUNCTION check_wallet_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE target uuid; total numeric; movement_count bigint; w wallets;
      BEGIN
        IF TG_TABLE_NAME='wallets' THEN target := NEW.id; ELSE target := NEW.wallet_id; END IF;
        SELECT * INTO STRICT w FROM wallets WHERE id=target;
        SELECT COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0),
          COUNT(*) FILTER (WHERE t.kind <> 'OPENING') INTO total,movement_count
          FROM ledger l JOIN wagers t ON t.id=l.transaction_id WHERE l.wallet_id=target;
        IF w.balance<>total OR w.version<>movement_count+1 THEN
          RAISE EXCEPTION 'wallet balance/version differs from ledger' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER wallet_consistency AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_consistency();
      CREATE CONSTRAINT TRIGGER ledger_consistency AFTER INSERT ON ledger
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_consistency();

      CREATE FUNCTION check_wager_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE t wagers; r wagers; entries bigint;
      BEGIN
        SELECT * INTO STRICT t FROM wagers WHERE id=NEW.id;
        SELECT COUNT(*) INTO entries FROM ledger WHERE transaction_id=t.id;
        IF entries <> (CASE WHEN t.status='PROCESSED' AND t.kind<>'LOSS' THEN 1 ELSE 0 END) THEN
          RAISE EXCEPTION 'wager ledger count mismatch' USING ERRCODE='23514';
        END IF;
        IF t.status='PROCESSED' AND t.reference_external_transaction_id IS NOT NULL THEN
          SELECT * INTO r FROM wagers WHERE id=t.reference_transaction_id;
          IF r.id IS NULL OR r.status<>'PROCESSED' OR
            ROW(r.provider_id,r.wallet_id,r.player_id,r.currency,r.round_id,r.external_transaction_id)
            IS DISTINCT FROM ROW(t.provider_id,t.wallet_id,t.player_id,t.currency,t.round_id,t.reference_external_transaction_id)
            OR (t.kind IN ('REFUND','WIN') AND r.kind<>'BET')
            OR (t.kind='ROLLBACK' AND r.kind NOT IN ('BET','WIN','REFUND'))
            OR (t.kind IN ('REFUND','ROLLBACK') AND t.amount<>r.amount) THEN
            RAISE EXCEPTION 'invalid processed reference' USING ERRCODE='23514';
          END IF;
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER wager_consistency AFTER INSERT OR UPDATE ON wagers
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wager_consistency();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      DROP TABLE outbox,inbox,ledger,wagers,wallets CASCADE;
      DROP FUNCTION check_wager_consistency(),check_wallet_consistency(),guard_ledger_insert(),guard_wager_transition(),deny_mutation();
    `);
  }
}
