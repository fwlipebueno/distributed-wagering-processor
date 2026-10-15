import { Migration } from "@mikro-orm/migrations";

export class Migration202610140003 extends Migration {
  override async up(): Promise<void> {
    // Deferred wager consistency counts entries by transaction across wallets.
    // The existing (wallet_id, transaction_id) index cannot serve that lookup.
    this.addSql("CREATE INDEX ledger_transaction_id ON ledger(transaction_id)");
  }

  override async down(): Promise<void> {
    this.addSql("DROP INDEX ledger_transaction_id");
  }
}
