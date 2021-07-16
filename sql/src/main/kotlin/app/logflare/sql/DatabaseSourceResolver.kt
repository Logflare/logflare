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
                   return Source(
                       name = source,
                       token = UUID.fromString(resultSet.getString(1)),
                       userId = userId
                   )
               }
           }
        }
    }

    override fun findByUUID(uuid: UUID): Source {
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT name FROM sources WHERE user_id = ? AND token = ?").use { stmt ->
                stmt.setLong(1, userId)
                stmt.setString(2, uuid.toString())
                stmt.executeQuery().use { resultSet ->
                    if (!resultSet.next()) {
                        throw SourceNotFound(uuid.toString(), userId)
                    }
                    return Source(
                        name = resultSet.getString(1),
                        token = uuid,
                        userId = userId
                    )
                }
            }
        }
    }
}
