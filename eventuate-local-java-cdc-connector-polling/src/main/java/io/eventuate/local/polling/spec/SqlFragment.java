package io.eventuate.local.polling.spec;

import java.util.Collections;
import java.util.Map;

public class SqlFragment {
    public final String sql;
    public final Map<String, Object> params;
    public static final SqlFragment EMPTY = new SqlFragment("", Collections.emptyMap());

    public SqlFragment(String sql, String placeholderName, Object value) {
        this(sql, Collections.singletonMap(placeholderName, value));
    }

    public SqlFragment(String sql, Map<String, Object> params) {
        this.sql = sql;
        this.params = params;
    }

    public static SqlFragment make(String sqlFormat, String column, String placeholderName, Object value) {
        return new SqlFragment(sqlFormat.formatted(column, ":" + placeholderName), placeholderName, value);
    }
}
