import { Migration } from "@mikro-orm/migrations";

export class Migration202610140002 extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      ALTER TABLE wallets DROP CONSTRAINT wallets_balance_check,
        ADD CONSTRAINT wallets_balance_check CHECK(balance >= 0 AND balance <> 'NaN'::numeric);
      ALTER TABLE wagers DROP CONSTRAINT wagers_amount_check,
        ADD CONSTRAINT wagers_amount_check CHECK(amount >= 0 AND amount <> 'NaN'::numeric);
      ALTER TABLE ledger DROP CONSTRAINT ledger_amount_check, DROP CONSTRAINT ledger_balance_before_check, DROP CONSTRAINT ledger_balance_after_check,
        ADD CONSTRAINT ledger_amount_check CHECK(amount > 0 AND amount <> 'NaN'::numeric),
        ADD CONSTRAINT ledger_balance_before_check CHECK(balance_before >= 0 AND balance_before <> 'NaN'::numeric),
        ADD CONSTRAINT ledger_balance_after_check CHECK(balance_after >= 0 AND balance_after <> 'NaN'::numeric);
      CREATE UNIQUE INDEX one_opening_per_wallet ON wagers(wallet_id) WHERE kind='OPENING';

      CREATE FUNCTION guard_wallet_identity() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF ROW(NEW.id,NEW.player_id,NEW.currency,NEW.created_at) IS DISTINCT FROM ROW(OLD.id,OLD.player_id,OLD.currency,OLD.created_at) THEN
          RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER wallet_identity BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION guard_wallet_identity();

      CREATE FUNCTION check_processed_wallet() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE t wagers; w wallets;
      BEGIN
        SELECT * INTO STRICT t FROM wagers WHERE id=NEW.id;
        SELECT * INTO STRICT w FROM wallets WHERE id=t.wallet_id;
        IF t.status='PROCESSED' AND (t.player_id<>w.player_id OR t.currency<>w.currency) THEN
          RAISE EXCEPTION 'processed wager wallet mismatch' USING ERRCODE='23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER processed_wallet_consistency AFTER INSERT OR UPDATE ON wagers
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_processed_wallet();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      DROP TRIGGER processed_wallet_consistency ON wagers;
      DROP TRIGGER wallet_identity ON wallets;
      DROP FUNCTION check_processed_wallet(),guard_wallet_identity();
      DROP INDEX one_opening_per_wallet;
      ALTER TABLE wallets DROP CONSTRAINT wallets_balance_check, ADD CONSTRAINT wallets_balance_check CHECK(balance >= 0);
      ALTER TABLE wagers DROP CONSTRAINT wagers_amount_check, ADD CONSTRAINT wagers_amount_check CHECK(amount >= 0);
      ALTER TABLE ledger DROP CONSTRAINT ledger_amount_check, DROP CONSTRAINT ledger_balance_before_check, DROP CONSTRAINT ledger_balance_after_check,
        ADD CONSTRAINT ledger_amount_check CHECK(amount > 0),
        ADD CONSTRAINT ledger_balance_before_check CHECK(balance_before >= 0),
        ADD CONSTRAINT ledger_balance_after_check CHECK(balance_after >= 0);
    `);
  }
}
