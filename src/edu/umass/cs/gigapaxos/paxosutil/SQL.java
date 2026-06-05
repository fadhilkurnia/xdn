/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         A file for just static SQL constants.
 */
public class SQL {
	/**
	 * The user name and password here are used by both gigapaxos and
	 * reconfiguration. Embedded derby can take any user name and password, so
	 * the values here should be for mysql or other DBs supported here.
	 * <p>
	 * These values are intentionally not configurable using gigapaxos.properties.
	 * We might support them using System command-line properties in the future.
	 */
	private static final String USER = Config.getGlobalString(PC.DB_USER); //"root";
	private static final String PASSWORD = Config.getGlobalString(PC.DB_PASSWORD); //"gnsRoot";

	/**
	 *
	 */
	public static enum SQLType {
		/**
		 *
		 */
		EMBEDDED_DERBY,
		/**
		 *
		 */
		MYSQL,

		/**
		 *
		 */
		EMBEDDED_H2,

		/**
		 * Embedded SQLite (via the xerial sqlite-jdbc driver). A lightweight,
		 * single-file, server-less embedded engine. Unlike Derby/H2 it is a
		 * single-writer engine, so it is best used with connection pooling
		 * disabled (see CONNECTION_POOLING) and WAL journaling enabled.
		 */
		EMBEDDED_SQLITE,
	};

	/**
	 * 
	 */
	public static final List<String> DUPLICATE_KEY = new ArrayList<String>(
			Arrays.asList("23505"));
	/**
	 * 
	 */
	public static final List<String> DUPLICATE_TABLE = new ArrayList<String>(
			Arrays.asList("X0Y32", "42S01", "42000", "42S11"));
	/**
	 * 
	 */
	public static final List<String> NONEXISTENT_TABLE = new ArrayList<String>(
			Arrays.asList("42Y07", "42Y55", "42X05", "42S02"));

	/**
	 * @param size
	 * @param type
	 * @return The string to use in create table statements.
	 */
	public static String getBlobString(int size, SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
		case EMBEDDED_H2:
			return " blob(" + size + ")";
		case EMBEDDED_SQLITE:
			// SQLite is dynamically typed; BLOB ignores any declared size.
			return " blob ";
		case MYSQL:
			if (size < 65536)
				return " TEXT ";
			else if (size < (int) Math.pow(2, 24))
				return " MEDIUMTEXT ";
			else if (size < (int) Math.pow(2, 32))
				return " LONGTEXT ";
		}
		return "";
	}
	
	/**
	 * @param table
	 * @param type
	 * @return String to get column schema of table.
	 */
	public static String getSchemaString(String table, SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
		case EMBEDDED_H2:
			return "select columnname, columndatatype from sys.syscolumns where referenceid in "
					+ "("
					+ "select tableid from sys.systables where tablename='"
					+ table.toUpperCase() + "'" + ")";
		case EMBEDDED_SQLITE:
			// pragma_table_info is a table-valued function (SQLite >= 3.16)
			// returning columns (cid, name, type, ...). We project name, type
			// so the result shape matches the (columnname, columndatatype)
			// contract expected by callers of this method. Table names in
			// SQLite are matched case-insensitively.
			return "select name, type from pragma_table_info('" + table + "')";
		case MYSQL:
			return "select column_name, column_type, column_default from information_schema.columns "
					+ "where table_schema=database() and table_name='"
					+ table.toUpperCase() + "'";
		default:
			return "";
		}
	}
	/**
	 * @param column
	 * @param type
	 * @return Alter column command as String
	 */
	public static String getAlterString(String column, SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
		case EMBEDDED_H2:
			return "alter column " + column + " set data type ";
		case EMBEDDED_SQLITE:
			// SQLite cannot change a column's declared type via ALTER TABLE.
			// In practice this branch is not exercised because SQLite stores
			// the declared type verbatim, so reconcileTable() reads back the
			// same type it created and detects no mismatch. We return the
			// MySQL-style form so that, if a genuine schema migration is ever
			// attempted, it fails loudly rather than silently.
			return "modify column " + column;
		case MYSQL:
			return "modify column " + column;
		default:
			return "";
		}
	}

	/**
	 * @param type
	 * @return Varchar size limit.
	 */
	public static int getVarcharSize(SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
		case EMBEDDED_H2:
		case EMBEDDED_SQLITE:
			// SQLite does not enforce VARCHAR limits; mirror Derby so that
			// blob-vs-varchar column-type decisions match the Derby-tested path.
			return 32672;
		case MYSQL:
			// 65535 in 5.1 onwards
			return 21845;
		}
		Util.suicide("SQL type not recognized");
		return -1;
	}

	/**
	 * @param type
	 * @return Driver string.
	 */
	public static String getDriver(SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
			return "org.apache.derby.jdbc.EmbeddedDriver";
		case MYSQL:
			return "com.mysql.jdbc.Driver";
		case EMBEDDED_H2:
			return "org.h2.Driver";
		case EMBEDDED_SQLITE:
			return "org.sqlite.JDBC";
		}
		Util.suicide("SQL type not recognized");
		return null;
	}

	/**
	 * @param type
	 * @return Protocol or URL string.
	 */
	public static String getProtocolOrURL(SQLType type) {
		switch (type) {
		case EMBEDDED_DERBY:
			return "jdbc:derby:";
		case MYSQL:
			return "jdbc:mysql://localhost/";
		case EMBEDDED_H2:
			return "jdbc:h2:";
		case EMBEDDED_SQLITE:
			return "jdbc:sqlite:";
		}
		Util.suicide("SQL type not recognized");
		return null;
	}

	/**
	 * @return Username.
	 */
	public static String getUser() {
		return USER;
	}

	/**
	 * @return Password.
	 */
	public static String getPassword() {
		return PASSWORD;
	}

	/* The classifiers below interpret a SQLException to decide whether it
	 * signals a duplicate primary key, a pre-existing table, or a missing
	 * table. Derby/H2/MySQL surface these via standard SQLState codes (the
	 * lists above). SQLite (xerial sqlite-jdbc) reports a null SQLState and a
	 * numeric vendor code instead, so for SQLite we additionally inspect the
	 * vendor error code and message text. Keeping this logic here means the
	 * SQLPaxosLogger/SQLReconfiguratorDB catch blocks stay vendor-agnostic. */

	private static boolean messageContains(SQLException e, String needle) {
		String m = e.getMessage();
		return m != null && m.toLowerCase().contains(needle);
	}

	/**
	 * @param e
	 * @return True if {@code e} indicates a duplicate/unique key violation.
	 */
	public static boolean isDuplicateKeyException(SQLException e) {
		if (DUPLICATE_KEY.contains(e.getSQLState()))
			return true;
		// SQLite: SQLITE_CONSTRAINT (19) and its extended UNIQUE/PRIMARYKEY
		// variants (2067, 1555).
		int code = e.getErrorCode();
		return code == 19 || code == 2067 || code == 1555
				|| messageContains(e, "unique constraint failed");
	}

	/**
	 * @param e
	 * @return True if {@code e} indicates the table already exists.
	 */
	public static boolean isDuplicateTableException(SQLException e) {
		if (DUPLICATE_TABLE.contains(e.getSQLState()))
			return true;
		// SQLite reports "table <name> already exists".
		return messageContains(e, "already exists");
	}

	/**
	 * @param e
	 * @return True if {@code e} indicates the table does not exist.
	 */
	public static boolean isNonexistentTableException(SQLException e) {
		if (NONEXISTENT_TABLE.contains(e.getSQLState()))
			return true;
		// SQLite reports "no such table: <name>".
		return messageContains(e, "no such table");
	}

}