package app.logflare.sql

import com.zaxxer.hikari.HikariDataSource
import java.util.*

class DatabaseSourceResolver(private val dataSource: HikariDataSource, private val userId: Long) : SourceResolver {
    override fun resolve(source: String): Source {
        dataSource.connection.use { conn ->
           conn.prepareStatement("SELECT token FROM sources WHERE user_id = ? AND name = ?").use { stmt ->
               stmt.setLong(1, userId)
               stmt.setString(2, source)
               stmt.executeQuery().use { resultSet ->
                   if (!resultSet.next()) {
                       throw SourceNotFound(source, userId)
                   }
                   return Source(token = UUID.fromString(resultSet.getString(1)), userId = userId)
               }
           }
        }
    }
}
