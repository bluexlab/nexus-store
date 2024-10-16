package model

import (
	"fmt"
	"strings"

	"gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
)

func CreateListFilter(s *nexus_store.ListFilter) (ListFilter, error) {
	switch s.GetType() {
	case nexus_store.ListFilter_AND_GROUP:
		filter := _AndGroupFilter{}
		for _, subFilter := range s.GetSubFilters() {
			f, err := CreateListFilter(subFilter)
			if err != nil {
				return nil, err
			}
			filter.filters = append(filter.filters, f)
		}
		return &filter, nil

	case nexus_store.ListFilter_OR_GROUP:
		filter := _OrGroupFilter{}
		for _, subFilter := range s.GetSubFilters() {
			f, err := CreateListFilter(subFilter)
			if err != nil {
				return nil, err
			}
			filter.filters = append(filter.filters, f)
		}
		return &filter, nil

	case nexus_store.ListFilter_EQUAL:
		if len(s.GetEntries()) <= 0 {
			return nil, fmt.Errorf("metadata entries is empty")

		} else if len(s.GetEntries()) > 1 {
			filter := _AndGroupFilter{}
			for _, entry := range s.GetEntries() {
				filter.filters = append(filter.filters, &_EqualFilter{key: entry.GetKey(), value: entry.GetValue()})
			}
			return &filter, nil
		} else {
			return &_EqualFilter{key: s.GetEntries()[0].GetKey(), value: s.GetEntries()[0].GetValue()}, nil
		}

	case nexus_store.ListFilter_NOT_EQUAL:
		if len(s.GetEntries()) <= 0 {
			return nil, fmt.Errorf("metadata entries is empty")

		} else if len(s.GetEntries()) > 1 {
			filter := _OrGroupFilter{}
			for _, entry := range s.GetEntries() {
				filter.filters = append(filter.filters, &_NotEqualFilter{key: entry.GetKey(), value: entry.GetValue()})
			}
			return &filter, nil

		} else {
			return &_NotEqualFilter{key: s.GetEntries()[0].GetKey(), value: s.GetEntries()[0].GetValue()}, nil
		}

	case nexus_store.ListFilter_CONTAINS:
		if len(s.GetEntries()) <= 0 {
			return nil, fmt.Errorf("metadata entries is empty")

		} else if len(s.GetEntries()) > 1 {
			filter := _AndGroupFilter{}
			for _, entry := range s.GetEntries() {
				filter.filters = append(filter.filters, &_ContainsFilter{key: entry.GetKey(), value: entry.GetValue()})
			}
			return &filter, nil

		} else {
			return &_ContainsFilter{key: s.GetEntries()[0].GetKey(), value: s.GetEntries()[0].GetValue()}, nil
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

// SqlQuery returns expression of WHERE.
func (f *_AndGroupFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateGroupToSQL(nexus_store.ListFilter_AND_GROUP, f.filters)
}

type _OrGroupFilter struct {
	filters []ListFilter
}

// SqlQuery returns expression of WHERE.
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

// SqlQuery returns expression of WHERE.
func (f *_NotEqualFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateEntriesToSQL(nexus_store.ListFilter_NOT_EQUAL, []nexus_store.MetadataEntry{{Key: f.key, Value: f.value}})
}

type _ContainsFilter struct {
	key   string
	value string
}

// SqlQuery returns expression of WHERE.
func (f *_ContainsFilter) SqlQuery() (*SqlQueryBuilder, error) {
	return translateEntriesToSQL(nexus_store.ListFilter_CONTAINS, []nexus_store.MetadataEntry{{Key: f.key, Value: f.value}})
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

	// Combine and deduplicate WHERE conditions
	// whereClause := strings.Join(removeDuplicates(strings.Split(strings.Join(whereConditions, " OR "), " OR ")), " OR ")
	whereClause := strings.Join(strings.Split(strings.Join(whereConditions, " OR "), " OR "), " OR ")

	// Combine HAVING conditions
	havingClause := strings.Join(havingConditions, conjunction)

	return &SqlQueryBuilder{WhereClause: whereClause, HavingClause: havingClause, WhereArgs: whereArgs, HavingArgs: havingArgs}, nil
}
