package com.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class DemoApplication {

		public static void main(String[] args) {

				time(() -> {

						List<Runnable> initializers = new ArrayList<>();

						DataSource ds = new EmbeddedDatabaseBuilder()
							.setType(EmbeddedDatabaseType.H2)
							.addScript("schema.sql")
							.build();

						JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
						DataSourceTransactionManager txm = new DataSourceTransactionManager(ds);
						TransactionTemplate transactionTemplate = new TransactionTemplate(txm);

						UserService userService = new UserService(jdbcTemplate, transactionTemplate);
						initializers.add(new UserRunner(userService));

						TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory(8080);
						WebServer webServer = tomcat.getWebServer(ctx -> {
								UserServlet userServlet = new UserServlet(userService);
								ctx.addServlet(userServlet.getClass().getSimpleName(), userServlet).addMapping("/");
						});
						webServer.start();

						initializers.forEach(Runnable::run);

				});

		}

		private static void time(Runnable r) {
				long start = System.currentTimeMillis();
				r.run();
				long stop = System.currentTimeMillis();
				LogFactory.getLog(DemoApplication.class).info("startup time: " + (stop - start) + "ms");
		}
}

class UserServlet extends HttpServlet {

		private final ObjectMapper om = new ObjectMapper();
		private final UserService userService;

		UserServlet(UserService userService) {
				this.userService = userService;
		}

		@Override
		protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
				if (req.getRequestURI().equalsIgnoreCase("/users")) {
						resp.getWriter().write(this.om.writeValueAsString(this.userService.get()));
				}
		}
}

class UserRunner implements Runnable {

		private final UserService userService;
		private final Log log = LogFactory.getLog(getClass());

		UserRunner(UserService userService) {
				this.userService = userService;
		}

		@Override
		public void run() {
				Stream.of("A", "B", "C").map(name -> new User(null, name)).forEach(this.userService::save);
				this.userService.get().forEach(usr -> this.userService.getById(usr.getId()));
		}
}

class UserService {

		private final JdbcTemplate jdbcTemplate;
		private final TransactionTemplate transactionTemplate;
		private final RowMapper<User> userRowMapper = (rs, rowNum) -> new User(rs.getLong("ID"), rs.getString("USERNAME"));

		UserService(JdbcTemplate jdbcTemplate, TransactionTemplate tt) {
				this.jdbcTemplate = jdbcTemplate;
				this.transactionTemplate = tt;
		}

		public void delete() {
				this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
						@Override
						protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
								jdbcTemplate.execute("delete from USERS");
						}
				});
		}

		public Collection<User> get() {
				return this.transactionTemplate
					.execute(s -> this.jdbcTemplate.query("select * from USERS", this.userRowMapper));
		}

		public User save(User u) {
				return this.transactionTemplate.execute(status -> {
						GeneratedKeyHolder kh = new GeneratedKeyHolder();
						this.jdbcTemplate.update(con -> {
								PreparedStatement psc1 = con
									.prepareStatement("insert into USERS(USERNAME) values(?)", new String[]{"ID"});
								psc1.setString(1, u.getUsername());
								return psc1;
						}, kh);

						return getById(kh.getKey().longValue());
				});
		}

		public User getById(Long id) {
				return findById(id).orElseThrow(() -> new IllegalArgumentException("couldn't find user # " + id));
		}

		public Optional<User> findById(Long id) {
				return this.transactionTemplate.execute(x -> {
						User user = this.jdbcTemplate.queryForObject("select * from USERS where ID = ? ", this.userRowMapper, id);
						return Optional.ofNullable(user);
				});
		}
}

@Data
@AllArgsConstructor
class User {
		private final Long id;
		private final String username;
}