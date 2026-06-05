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
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A minimal, dependency-free {@link DataSource} with a small bounded pool of
 * reused physical connections. It spawns no background/helper threads and holds
 * at most {@link #maxIdle} idle connections, so its memory and thread footprint
 * is far smaller than a C3P0 {@code ComboPooledDataSource} (which starts ~3
 * helper threads per data source). At the same time, by reusing connections it
 * avoids paying the per-operation connection-open cost &mdash; which is
 * significant for SQLite, where opening a connection re-applies pragmas such as
 * journal_mode=WAL.
 * <p>
 * Borrowed connections are returned to the pool when the caller invokes
 * {@link Connection#close()} (intercepted via a dynamic proxy); gigapaxos
 * already routes all acquisition through a couple of helper methods and closes
 * connections in finally blocks, so this is a drop-in replacement. If the idle
 * pool is full on return, or the connection is broken, it is physically closed.
 * <p>
 * Set {@code maxIdle} to 0 for purely non-pooling behavior (a fresh connection
 * per op, closed on return). This is the lowest-memory option but pays the
 * connection-open cost on every operation.
 *
 * @author arun
 */
public class SimpleDataSource implements DataSource {

	private volatile String jdbcUrl;
	private final Properties props;
	private final int maxIdle;
	private final ConcurrentLinkedQueue<Connection> idle = new ConcurrentLinkedQueue<Connection>();
	private final AtomicInteger idleCount = new AtomicInteger(0);
	// when non-null, bounds the number of concurrently outstanding
	// connections; a permit count of 1 yields a single serialized connection
	// (a "single writer"), which avoids SQLite WAL cross-connection
	// SQLITE_BUSY_SNAPSHOT conflicts at the cost of node-local DB concurrency.
	private final Semaphore permits;

	private PrintWriter logWriter = null;
	private int loginTimeout = 0;

	/**
	 * @param jdbcUrl
	 * @param props
	 *            connection properties (e.g. user, password, and any
	 *            engine-specific pragmas); may be null.
	 * @param maxIdle
	 *            maximum number of idle physical connections to keep; 0 means
	 *            no pooling (fresh connection per op).
	 * @param maxTotal
	 *            maximum number of concurrently outstanding connections; 0
	 *            means unbounded. Set to 1 for a single serialized connection.
	 */
	public SimpleDataSource(String jdbcUrl, Properties props, int maxIdle,
			int maxTotal) {
		this.jdbcUrl = jdbcUrl;
		this.props = props != null ? props : new Properties();
		this.maxIdle = Math.max(0, maxIdle);
		this.permits = maxTotal > 0 ? new Semaphore(maxTotal, true) : null;
	}

	/**
	 * @param jdbcUrl
	 * @param props
	 * @param maxIdle
	 */
	public SimpleDataSource(String jdbcUrl, Properties props, int maxIdle) {
		this(jdbcUrl, props, maxIdle, 0);
	}

	/**
	 * @param jdbcUrl
	 * @param props
	 */
	public SimpleDataSource(String jdbcUrl, Properties props) {
		this(jdbcUrl, props, 8, 0);
	}

	/**
	 * @param jdbcUrl
	 *            the JDBC URL to use for subsequently created connections. Used
	 *            to mirror C3P0's setJdbcUrl(.) so that callers can strip an
	 *            initial ";create=true" style flag after the first connect.
	 */
	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	/**
	 * @return the current JDBC URL.
	 */
	public String getJdbcUrl() {
		return this.jdbcUrl;
	}

	@Override
	public Connection getConnection() throws SQLException {
		// bound concurrency if configured (blocks until a permit is free)
		if (permits != null) {
			try {
				permits.acquire();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new SQLException("interrupted acquiring connection permit",
						e);
			}
		}
		try {
			Connection physical = null;
			// reuse an idle connection if one is available and still usable
			while ((physical = idle.poll()) != null) {
				idleCount.decrementAndGet();
				if (isUsable(physical))
					return wrap(physical);
				quietClose(physical);
			}
			// else open a fresh physical connection
			physical = DriverManager.getConnection(this.jdbcUrl, this.props);
			return wrap(physical);
		} catch (SQLException | RuntimeException e) {
			if (permits != null)
				permits.release();
			throw e;
		}
	}

	@Override
	public Connection getConnection(String username, String password)
			throws SQLException {
		Properties p = new Properties();
		p.putAll(this.props);
		if (username != null)
			p.put("user", username);
		if (password != null)
			p.put("password", password);
		// distinct credentials bypass the pool
		return wrap(DriverManager.getConnection(this.jdbcUrl, p));
	}

	/* Wraps a physical connection in a proxy whose close() returns it to the
	 * idle pool (instead of closing it) when there is room. */
	private Connection wrap(final Connection physical) {
		return (Connection) Proxy.newProxyInstance(
				SimpleDataSource.class.getClassLoader(),
				new Class<?>[] { Connection.class },
				new InvocationHandler() {
					private boolean returned = false;

					@Override
					public Object invoke(Object proxy, Method method,
							Object[] args) throws Throwable {
						String name = method.getName();
						if ("close".equals(name)) {
							release(physical);
							returned = true;
							return null;
						}
						if ("isClosed".equals(name) && returned)
							return true;
						try {
							return method.invoke(physical, args);
						} catch (InvocationTargetException e) {
							throw e.getCause();
						}
					}
				});
	}

	/* Returns a connection to the idle pool, or physically closes it if the
	 * pool is full or the connection is no longer usable. Always releases a
	 * concurrency permit (if bounded) exactly once per borrowed connection. */
	private void release(Connection physical) {
		try {
			if (maxIdle > 0 && idleCount.get() < maxIdle && !physical.isClosed()) {
				if (!physical.getAutoCommit()) {
					try {
						physical.rollback();
					} catch (SQLException e) {
						// ignore; will reset below
					}
					physical.setAutoCommit(true);
				}
				idle.offer(physical);
				idleCount.incrementAndGet();
				return;
			}
		} catch (SQLException e) {
			// fall through to close
		} finally {
			if (permits != null)
				permits.release();
		}
		quietClose(physical);
	}

	private static boolean isUsable(Connection c) {
		try {
			return !c.isClosed();
		} catch (SQLException e) {
			return false;
		}
	}

	private static void quietClose(Connection c) {
		try {
			c.close();
		} catch (SQLException e) {
			// ignore
		}
	}

	@Override
	public PrintWriter getLogWriter() {
		return this.logWriter;
	}

	@Override
	public void setLogWriter(PrintWriter out) {
		this.logWriter = out;
	}

	@Override
	public void setLoginTimeout(int seconds) {
		this.loginTimeout = seconds;
	}

	@Override
	public int getLoginTimeout() {
		return this.loginTimeout;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this))
			return iface.cast(this);
		throw new SQLException(this.getClass().getName()
				+ " is not a wrapper for " + iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return iface.isInstance(this);
	}
}
