package conv

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

func ToString(o interface{}) string {
	if o == nil {
		return ""
	}
	switch o.(type) {
	case string:
		return o.(string)
	case time.Time:
		var x time.Time
		if !o.(time.Time).Equal(x) {
			st, _ := (o.(time.Time)).MarshalText()
			return string(st)
		} else {
			// NULL Date will return empty string
			return ""
		}
	case uint8:
		return strconv.FormatUint(uint64(o.(byte)), 10)
	case uint16:
		return strconv.FormatUint(uint64(o.(uint16)), 10)
	case uint32:
		return strconv.FormatUint(uint64(o.(uint32)), 10)
	case uint64:
		return strconv.FormatUint(uint64(o.(uint64)), 10)
	case uint:
		return strconv.FormatUint(uint64(o.(uint)), 10)
	case int8:
		return strconv.FormatInt(int64(o.(int8)), 10)
	case int16:
		return strconv.FormatInt(int64(o.(int16)), 10)
	case int32:
		return strconv.FormatInt(int64(o.(int32)), 10)
	case int64:
		return strconv.FormatInt(int64(o.(int64)), 10)
	case int:
		return strconv.FormatInt(int64(o.(int)), 10)
	case float32:
		return strconv.FormatFloat(float64(o.(float32)), 'E', -1, 64)
	case float64:
		return strconv.FormatFloat(float64(o.(float64)), 'E', -1, 64)
	default:
		return "[Object]"
	}
}

func ToFloat64(o interface{}) float64 {
	switch o.(type) {
	case float64:
		return o.(float64)
	case float32:
		return float64(o.(float32))
	case uint8:
		return float64(o.(byte))
	case uint16:
		return float64(o.(uint16))
	case uint32:
		return float64(o.(uint32))
	case uint64:
		return float64(o.(uint64))
	case uint:
		return float64(o.(uint))
	case int8:
		return float64(o.(int8))
	case int16:
		return float64(o.(int16))
	case int32:
		return float64(o.(int32))
	case int64:
		return float64(o.(int64))
	case int:
		return float64(o.(int))
	case string:
		r := float64(0)
		r, _ = strconv.ParseFloat(ToString(o), 64)
		return r
	default:
		return float64(0)
	}
}

func ToInt64(o interface{}) int64 {
	switch o.(type) {
	case float64:
		return int64(o.(float64))
	case float32:
		return int64(o.(float32))
	case uint8:
		return int64(o.(byte))
	case uint16:
		return int64(o.(uint16))
	case uint32:
		return int64(o.(uint32))
	case uint64:
		return int64(o.(uint64))
	case uint:
		return int64(o.(uint))
	case int8:
		return int64(o.(int8))
	case int16:
		return int64(o.(int16))
	case int32:
		return int64(o.(int32))
	case int64:
		return int64(o.(int64))
	case int:
		return int64(o.(int))
	case string:
		r := int64(0)
		r, _ = strconv.ParseInt(ToString(o), 10, 64)
		return int64(r)
	default:
		return int64(0)
	}
}

func ToUint64(o interface{}) uint64 {
	switch o.(type) {
	case float64:
		return uint64(o.(float64))
	case float32:
		return uint64(o.(float32))
	case uint8:
		return uint64(o.(byte))
	case uint16:
		return uint64(o.(uint16))
	case uint32:
		return uint64(o.(uint32))
	case uint64:
		return uint64(o.(uint64))
	case uint:
		return uint64(o.(uint))
	case int8:
		return uint64(o.(int8))
	case int16:
		return uint64(o.(int16))
	case int32:
		return uint64(o.(int32))
	case int64:
		return uint64(o.(int64))
	case int:
		return uint64(o.(int))
	case string:
		r := uint64(0)
		r, _ = strconv.ParseUint(ToString(o), 10, 64)
		return uint64(r)
	default:
		return uint64(0)
	}
}

func ToBool(o interface{}) bool {
	switch o.(type) {
	case bool:
		return o.(bool)
	case uint8:
		return o.(byte) != 0
	case uint16:
		return o.(uint16) != 0
	case uint32:
		return o.(uint32) != 0
	case uint64:
		return o.(uint64) != 0
	case uint:
		return o.(uint) != 0
	case int8:
		return o.(int8) != 0
	case int16:
		return o.(int16) != 0
	case int32:
		return o.(int32) != 0
	case int64:
		return o.(int64) != 0
	case int:
		return o.(int) != 0
	case string:
		r := uint64(0)
		if rb, err := strconv.ParseBool(o.(string)); err == nil {
			return rb
		}
		r, _ = strconv.ParseUint(ToString(o), 10, 64)
		return r != 0
	default:
		return false
	}
}

func parseDateExp(s string) (time.Time, error) {
	ret := time.Now()
	var d byte
	var add int
	var unit byte

	cnt, e := fmt.Sscanf(s, "%c%d%c", &d, &add, &unit)
	if d == 'd' || d == 'D' || d == 't' || d == 'T' {
		switch cnt {
		case 1:
			ret = time.Now().Local()
			e = nil
			break
		case 2:
			ret = time.Now().AddDate(0, 0, add)
			break
		case 3:
			switch unit {
			case 'd', 'D':
				ret = time.Now().AddDate(0, 0, add)
			case 'w', 'W':
				ret = time.Now().AddDate(0, 0, add*7)
			case 'm', 'M':
				ret = time.Now().AddDate(0, add, 0)
			case 'y', 'Y':
				ret = time.Now().AddDate(add, 0, 0)
			default:
				ret = *(new(time.Time))
				e = errors.New("Invalid format")
			}
			break
		default:
			ret = *(new(time.Time))
			e = errors.New("Invalid format")
		}
	} else {
		e = errors.New("Invalid format")
	}

	return ret, e

}

func ToTime(o interface{}) time.Time {
	switch o.(type) {
	case string:
		if d, e := parseDateExp(o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("2006-01-02 15:04:05 (MST)", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("2006-01-02 15:04:05", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("2006-01-02 15:04", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("2006-01-02", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("02-Jan-2006 15:04.02", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("02-Jan-2006 15:04.02 (MST)", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("02-Jan-2006 15:04", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("02-Jan-2006", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("2006-01-02T15:04:05Z", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("15:04:05", o.(string)); e == nil {
			return d
		}
		if d, e := time.Parse("15:04", o.(string)); e == nil {
			return d
		}
	case time.Time:
		return o.(time.Time)

	}
	var v time.Time
	return v
}

func StructToMap(o interface{}) map[string]interface{} {
	p := make(map[string]interface{})
	v := reflect.Indirect(reflect.ValueOf(o))
	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		pname := "@" + vt.Field(i).Name
		p[pname] = v.Field(i).Interface()
	}
	return p
}

func ToJson(x interface{}) string {
	if j, e := json.Marshal(x); e == nil {
		return string(j)
	} else {
		return ""
	}

}
