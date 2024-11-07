package model

import (
	"fmt"
	"regexp"
	"strings"

	"gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
)

func BuildListQuery(inclusion *nexus_store.ListFilter, exclusion *nexus_store.ListFilter) (string, []interface{}, error) {
	includeFilter, err := createListFilter(inclusion)
	if err != nil {
		return "", nil, err
	}

	exclusionFilter, err := createListFilter(exclusion)
	if err != nil {
		return "", nil, err
	}

	builder, err := includeFilter.SqlQuery()
	if err != nil {
		return "", nil, err
	}

	var sqlQuery string
	var args []interface{}
	if exclusionFilter != nil {
		exclusionBuilder, err := exclusionFilter.SqlQuery()
		if err != nil {
			return "", nil, err
		}

		sqlQuery = fmt.Sprintf(
			`WITH exclusion AS (SELECT object_id, document_id FROM metadatas WHERE %s GROUP BY object_id, document_id HAVING %s)
			 SELECT object_id, document_id FROM metadatas WHERE NOT EXISTS (SELECT 1 FROM exclusion e WHERE e.object_id IS NOT DISTINCT FROM metadatas.object_id AND e.document_id IS NOT DISTINCT FROM metadatas.document_id)
			 AND (%s) GROUP BY object_id, document_id HAVING %s;`,
			exclusionBuilder.WhereClause,
			exclusionBuilder.HavingClause,
			builder.WhereClause,
			builder.HavingClause,
		)
		args = append(exclusionBuilder.WhereArgs, exclusionBuilder.HavingArgs...)
		args = append(args, builder.WhereArgs...)
		args = append(args, builder.HavingArgs...)

	} else {
		sqlQuery = fmt.Sprintf(
			"SELECT object_id, document_id FROM metadatas WHERE %s GROUP BY object_id, document_id HAVING %s;",
			builder.WhereClause,
			builder.HavingClause,
		)
		args = append(builder.WhereArgs, builder.HavingArgs...)
	}

	// replace ? with $1, $2, ...
	paramCount := 1
	sqlQuery = regexp.MustCompile(`\?`).ReplaceAllStringFunc(sqlQuery, func(string) string {
		placeholder := fmt.Sprintf("$%d", paramCount)
		paramCount++
		return placeholder
	})
	return sqlQuery, args, nil
}

func createListFilter(s *nexus_store.ListFilter) (ListFilter, error) {
	if s == nil {
		return nil, nil
	}

	switch s.GetType() {
	case nexus_store.ListFilter_AND_GROUP:
		filter := _AndGroupFilter{}
		for _, subFilter := range s.GetSubFilters() {
			f, err := createListFilter(subFilter)
			if err != nil {
				return nil, err
			}
			filter.filters = append(filter.filters, f)
		}
		return &filter, nil

	case nexus_store.ListFilter_OR_GROUP:
		filter := _OrGroupFilter{}
		for _, subFilter := range s.GetSubFilters() {
			f, err := createListFilter(subFilter)
			if err != nil {
				return nil, err
			}
			filter.filters = append(filter.filters, f)
		}
		return &filter, nil

	case nexus_store.ListFilter_EQUAL:
		if len(s.GetEntries()) > 1 {
			filter := _AndGroupFilter{}
			for key, value := range s.GetEntries() {
				filter.filters = append(filter.filters, &_EqualFilter{key: key, value: value})
			}
			return &filter, nil
		} else {
			for key, value := range s.GetEntries() {
				return &_EqualFilter{key: key, value: value}, nil
			}
			return nil, fmt.Errorf("unexpected empty entries")
		}

	case nexus_store.ListFilter_NOT_EQUAL:
		if len(s.GetEntries()) > 1 {
			filter := _OrGroupFilter{}
			for key, value := range s.GetEntries() {
				filter.filters = append(filter.filters, &_NotEqualFilter{key: key, value: value})
			}
			return &filter, nil

		} else {
			for key, value := range s.GetEntries() {
				return &_NotEqualFilter{key: key, value: value}, nil
			}
			return nil, fmt.Errorf("unexpected empty entries")
		}

	case nexus_store.ListFilter_CONTAINS:
		if len(s.GetEntries()) > 1 {
			filter := _AndGroupFilter{}
			for key, value := range s.GetEntries() {
				filter.filters = append(filter.filters, &_ContainsFilter{key: key, value: value})
			}
			return &filter, nil

		} else {
			for key, value := range s.GetEntries() {
				return &_ContainsFilter{key: key, value: value}, nil
			}
			return nil, fmt.Errorf("unexpected empty entries")
		}

	case nexus_store.ListFilter_HAS_KEYS:
		if len(s.GetEntries()) > 1 {
			filter := _AndGroupFilter{}
			for key := range s.GetEntries() {
				filter.filters = append(filter.filters, &_HasKeysFilter{key: key})
			}
			return &filter, nil
		} else {
			for key := range s.GetEntries() {
				return &_HasKeysFilter{key: key}, nil
			}
			return nil, fmt.Errorf("unexpected empty entries")
		}

	default:
		return nil, fmt.Errorf("invalid filter type: %v", s.GetType())
	}
}

// StatusFilter is an interface to generate SQL query for WHERE expression
type ListFilter interface {
	SqlQuery() (*SqlQueryBuilder, error)
}

type SqlQueryBuilder struct {
	WhereClause  string
	HavingClause string
	WhereArgs    []interface{}
	HavingArgs   []interface{}
}

type _AndGroupFilter struct {
	filters []ListFilter
}

func (f *_AndGroupFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateGroupToSQL(nexus_store.ListFilter_AND_GROUP, f.filters)
}

type _OrGroupFilter struct {
	filters []ListFilter
}

func (f *_OrGroupFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateGroupToSQL(nexus_store.ListFilter_OR_GROUP, f.filters)
}

type _EqualFilter struct {
	key   string
	value string
}

func (f *_EqualFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateEntriesToSQL(nexus_store.ListFilter_EQUAL, []nexus_store.MetadataEntry{{Key: f.key, Value: f.value}})
}

type _NotEqualFilter struct {
	key   string
	value string
}

func (f *_NotEqualFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateEntriesToSQL(nexus_store.ListFilter_NOT_EQUAL, []nexus_store.MetadataEntry{{Key: f.key, Value: f.value}})
}

type _ContainsFilter struct {
	key   string
	value string
}

func (f *_ContainsFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateEntriesToSQL(nexus_store.ListFilter_CONTAINS, []nexus_store.MetadataEntry{{Key: f.key, Value: f.value}})
}

type _HasKeysFilter struct {
	key string
}

func (f *_HasKeysFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateEntriesToSQL(nexus_store.ListFilter_HAS_KEYS, []nexus_store.MetadataEntry{{Key: f.key}})
}

func translateEntriesToSQL(filterType nexus_store.ListFilter_Type, entries []nexus_store.MetadataEntry) (*SqlQueryBuilder, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("entries cannot be empty for filter type %v", filterType)
	}

	var whereConditions []string
	var havingConditions []string
	var whereArgs []interface{}
	var havingArgs []interface{}

	for _, entry := range entries {
		whereConditions = append(whereConditions, "key = ?")
		whereArgs = append(whereArgs, entry.GetKey())

		// Build HAVING condition based on filter type
		var havingCondition string
		switch filterType {
		case nexus_store.ListFilter_EQUAL:
			havingCondition = "SUM(CASE WHEN key = ? AND value = ? THEN 1 ELSE 0 END) > 0"
			havingArgs = append(havingArgs, entry.GetKey(), entry.GetValue())
		case nexus_store.ListFilter_NOT_EQUAL:
			havingCondition = "SUM(CASE WHEN key = ? AND value != ? THEN 1 ELSE 0 END) > 0"
			havingArgs = append(havingArgs, entry.GetKey(), entry.GetValue())
		case nexus_store.ListFilter_CONTAINS:
			havingCondition = "SUM(CASE WHEN key = ? AND value LIKE ? THEN 1 ELSE 0 END) > 0"
			havingArgs = append(havingArgs, entry.GetKey(), fmt.Sprintf("%%%s%%", entry.GetValue()))
		case nexus_store.ListFilter_HAS_KEYS:
			havingCondition = "SUM(CASE WHEN key = ? THEN 1 ELSE 0 END) > 0"
			havingArgs = append(havingArgs, entry.GetKey())
		default:
			return nil, fmt.Errorf("unsupported filter type in entries: %v", filterType)
		}
		havingConditions = append(havingConditions, havingCondition)
	}

	// Remove duplicate WHERE conditions
	// whereClause := strings.Join(removeDuplicates(whereConditions), " OR ")
	whereClause := strings.Join(whereConditions, " OR ")
	havingClause := strings.Join(havingConditions, " AND ")

	return &SqlQueryBuilder{WhereClause: whereClause, HavingClause: havingClause, WhereArgs: whereArgs, HavingArgs: havingArgs}, nil
}

func translateGroupToSQL(filterType nexus_store.ListFilter_Type, subFilters []ListFilter) (*SqlQueryBuilder, error) {
	if len(subFilters) == 0 {
		return nil, fmt.Errorf("sub_filters cannot be empty for group type %v", filterType)
	}

	var whereConditions []string
	var havingConditions []string
	var whereArgs []interface{}
	var havingArgs []interface{}

	for _, subFilter := range subFilters {
		builder, err := subFilter.SqlQuery()
		if err != nil {
			return nil, err
		}

		// Collect WHERE conditions
		if builder.WhereClause != "" {
			whereConditions = append(whereConditions, builder.WhereClause)
			whereArgs = append(whereArgs, builder.WhereArgs...)
		}

		// Collect HAVING conditions, wrapping them in parentheses
		if builder.HavingClause != "" {
			havingConditions = append(havingConditions, "("+builder.HavingClause+")")
			havingArgs = append(havingArgs, builder.HavingArgs...)
		}
	}

	var conjunction string
	if filterType == nexus_store.ListFilter_OR_GROUP {
		conjunction = " OR "
	} else {
		conjunction = " AND "
	}

	// Combine WHERE conditions
	whereClause := strings.Join(whereConditions, " OR ")

	// Combine HAVING conditions
	havingClause := strings.Join(havingConditions, conjunction)

	return &SqlQueryBuilder{WhereClause: whereClause, HavingClause: havingClause, WhereArgs: whereArgs, HavingArgs: havingArgs}, nil
}
