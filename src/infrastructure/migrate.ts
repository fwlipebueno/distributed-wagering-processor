import { connectDatabase } from "./database.js";

const orm = await connectDatabase(
  process.env.DATABASE_URL ?? "postgresql://wager:wager@localhost:55432/wager",
);
try {
  if (process.argv[2] === "down") await orm.getMigrator().down();
  else if (process.argv[2] === "up") await orm.getMigrator().up();
  else throw new Error("Use migrate.ts up|down");
} finally {
  await orm.close(true);
}
