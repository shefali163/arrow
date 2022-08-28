// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrow

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/xerrors"
)

type BooleanType struct{}

func (t *BooleanType) ID() Type            { return BOOL }
func (t *BooleanType) Name() string        { return "bool" }
func (t *BooleanType) String() string      { return "bool" }
func (t *BooleanType) Fingerprint() string { return typeFingerprint(t) }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *BooleanType) BitWidth() int { return 1 }

func (BooleanType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecBitmap()}}
}

type FixedSizeBinaryType struct {
	ByteWidth int
}

func (*FixedSizeBinaryType) ID() Type              { return FIXED_SIZE_BINARY }
func (*FixedSizeBinaryType) Name() string          { return "fixed_size_binary" }
func (t *FixedSizeBinaryType) BitWidth() int       { return 8 * t.ByteWidth }
func (t *FixedSizeBinaryType) Fingerprint() string { return typeFingerprint(t) }
func (t *FixedSizeBinaryType) String() string {
	return "fixed_size_binary[" + strconv.Itoa(t.ByteWidth) + "]"
}
func (t *FixedSizeBinaryType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(t.ByteWidth)}}
}

type (
	Timestamp int64
	Time32    int32
	Time64    int64
	TimeUnit  int
	Date32    int32
	Date64    int64
	Duration  int64
)

// Date32FromTime returns a Date32 value from a time object
func Date32FromTime(t time.Time) Date32 {
	if _, offset := t.Zone(); offset != 0 {
		// properly account for timezone adjustments before we calculate
		// the number of days by adjusting the time and converting to UTC
		t = t.Add(time.Duration(offset) * time.Second).UTC()
	}
	return Date32(t.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
}

func (d Date32) ToTime() time.Time {
	return time.Unix(0, 0).UTC().AddDate(0, 0, int(d))
}

func (d Date32) FormattedString() string {
	return d.ToTime().Format("2006-01-02")
}

// Date64FromTime returns a Date64 value from a time object
func Date64FromTime(t time.Time) Date64 {
	if _, offset := t.Zone(); offset != 0 {
		// properly account for timezone adjustments before we calculate
		// the actual value by adjusting the time and converting to UTC
		t = t.Add(time.Duration(offset) * time.Second).UTC()
	}
	// truncate to the start of the day to get the correct value
	t = t.Truncate(24 * time.Hour)
	return Date64(t.Unix()*1e3 + int64(t.Nanosecond())/1e6)
}

func (d Date64) ToTime() time.Time {
	days := int(int64(d) / (time.Hour * 24).Milliseconds())
	return time.Unix(0, 0).UTC().AddDate(0, 0, days)
}

func (d Date64) FormattedString() string {
	return d.ToTime().Format("2006-01-02")
}

// TimestampFromStringInLocation is like TimestampFromString, but treats the time instant
// as if it were in the passed timezone before converting to UTC for internal representation.
func TimestampFromStringInLocation(val string, unit TimeUnit, loc *time.Location) (Timestamp, error) {
	if len(val) < 10 {
		return 0, fmt.Errorf("invalid timestamp string")
	}

	var (
		format         = "2006-01-02"
		zoneFmt        string
		lenWithoutZone = len(val)
	)

	if lenWithoutZone > 10 {
		switch {
		case val[len(val)-1] == 'Z':
			zoneFmt = "Z"
			lenWithoutZone--
		case val[len(val)-3] == '+' || val[len(val)-3] == '-':
			zoneFmt = "-07"
			lenWithoutZone -= 3
		case val[len(val)-5] == '+' || val[len(val)-5] == '-':
			zoneFmt = "-0700"
			lenWithoutZone -= 5
		case val[len(val)-6] == '+' || val[len(val)-6] == '-':
			zoneFmt = "-07:00"
			lenWithoutZone -= 6
		}
	}

	switch {
	case lenWithoutZone == 13:
		format += string(val[10]) + "15"
	case lenWithoutZone == 16:
		format += string(val[10]) + "15:04"
	case lenWithoutZone >= 19:
		format += string(val[10]) + "15:04:05.999999999"
	}

	// error if we're truncating precision
	// don't need a case for nano as time.Parse will already error if
	// more than nanosecond precision is provided
	switch {
	case unit == Second && lenWithoutZone > 19:
		return 0, xerrors.New("provided more than second precision for timestamp[s]")
	case unit == Millisecond && lenWithoutZone > 23:
		return 0, xerrors.New("provided more than millisecond precision for timestamp[ms]")
	case unit == Microsecond && lenWithoutZone > 26:
		return 0, xerrors.New("provided more than microsecond precision for timestamp[us]")
	}

	format += zoneFmt
	out, err := time.Parse(format, val)
	if err != nil {
		return 0, err
	}
	if loc != time.UTC {
		// convert to UTC by putting the same time instant in the desired location
		// before converting to UTC
		out = out.In(loc).UTC()
	}

	switch unit {
	case Second:
		return Timestamp(out.Unix()), nil
	case Millisecond:
		return Timestamp(out.Unix()*1e3 + int64(out.Nanosecond())/1e6), nil
	case Microsecond:
		return Timestamp(out.Unix()*1e6 + int64(out.Nanosecond())/1e3), nil
	case Nanosecond:
		return Timestamp(out.UnixNano()), nil
	}
	return 0, fmt.Errorf("unexpected timestamp unit: %s", unit)
}

// TimestampFromString parses a string and returns a timestamp for the given unit
// level.
//
// The timestamp should be in one of the following forms, [T] can be either T
// or a space, and [.zzzzzzzzz] can be either left out or up to 9 digits of
// fractions of a second.
//
//	 YYYY-MM-DD
//	 YYYY-MM-DD[T]HH
//   YYYY-MM-DD[T]HH:MM
//   YYYY-MM-DD[T]HH:MM:SS[.zzzzzzzz]
//
// You can also optionally have an ending Z to indicate UTC or indicate a specific
// timezone using ±HH, ±HHMM or ±HH:MM at the end of the string.
func TimestampFromString(val string, unit TimeUnit) (Timestamp, error) {
	return TimestampFromStringInLocation(val, unit, time.UTC)
}

func (t Timestamp) ToTime(unit TimeUnit) time.Time {
	if unit == Second {
		return time.Unix(int64(t), 0).UTC()
	}
	return time.Unix(0, int64(t)*int64(unit.Multiplier())).UTC()
}

// Time32FromString parses a string to return a Time32 value in the given unit,
// unit needs to be only seconds or milliseconds and the string should be in the
// form of HH:MM or HH:MM:SS[.zzz] where the fractions of a second are optional.
func Time32FromString(val string, unit TimeUnit) (Time32, error) {
	switch unit {
	case Second:
		if len(val) > 8 {
			return 0, xerrors.New("cannot convert larger than second precision to time32s")
		}
	case Millisecond:
		if len(val) > 12 {
			return 0, xerrors.New("cannot convert larger than millisecond precision to time32ms")
		}
	case Microsecond, Nanosecond:
		return 0, xerrors.New("time32 can only be seconds or milliseconds")
	}

	var (
		out time.Time
		err error
	)
	switch {
	case len(val) == 5:
		out, err = time.Parse("15:04", val)
	default:
		out, err = time.Parse("15:04:05.999", val)
	}
	if err != nil {
		return 0, err
	}
	t := out.Sub(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))
	if unit == Second {
		return Time32(t.Seconds()), nil
	}
	return Time32(t.Milliseconds()), nil
}

func (t Time32) ToTime(unit TimeUnit) time.Time {
	return time.Unix(0, int64(t)*int64(unit.Multiplier())).UTC()
}

func (t Time32) FormattedString(unit TimeUnit) string {
	const baseFmt = "15:04:05"
	tm := t.ToTime(unit)
	switch unit {
	case Second:
		return tm.Format(baseFmt)
	case Millisecond:
		return tm.Format(baseFmt + ".000")
	}
	return ""
}

// Time64FromString parses a string to return a Time64 value in the given unit,
// unit needs to be only microseconds or nanoseconds and the string should be in the
// form of HH:MM or HH:MM:SS[.zzzzzzzzz] where the fractions of a second are optional.
func Time64FromString(val string, unit TimeUnit) (Time64, error) {
	// don't need to check length for nanoseconds as Parse will already error
	// if more than 9 digits are provided for the fractional second
	switch unit {
	case Microsecond:
		if len(val) > 15 {
			return 0, xerrors.New("cannot convert larger than microsecond precision to time64us")
		}
	case Second, Millisecond:
		return 0, xerrors.New("time64 should only be microseconds or nanoseconds")
	}

	var (
		out time.Time
		err error
	)
	switch {
	case len(val) == 5:
		out, err = time.Parse("15:04", val)
	default:
		out, err = time.Parse("15:04:05.999999999", val)
	}
	if err != nil {
		return 0, err
	}
	t := out.Sub(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC))
	if unit == Microsecond {
		return Time64(t.Microseconds()), nil
	}
	return Time64(t.Nanoseconds()), nil
}

func (t Time64) ToTime(unit TimeUnit) time.Time {
	return time.Unix(0, int64(t)*int64(unit.Multiplier())).UTC()
}

func (t Time64) FormattedString(unit TimeUnit) string {
	const baseFmt = "15:04:05.000000"
	tm := t.ToTime(unit)
	switch unit {
	case Microsecond:
		return tm.Format(baseFmt)
	case Nanosecond:
		return tm.Format(baseFmt + "000")
	}
	return ""
}

const (
	Second TimeUnit = iota
	Millisecond
	Microsecond
	Nanosecond
)

func (u TimeUnit) Multiplier() time.Duration {
	return [...]time.Duration{time.Second, time.Millisecond, time.Microsecond, time.Nanosecond}[uint(u)&3]
}

func (u TimeUnit) String() string { return [...]string{"s", "ms", "us", "ns"}[uint(u)&3] }

type TemporalWithUnit interface {
	FixedWidthDataType
	TimeUnit() TimeUnit
}

// TimestampType is encoded as a 64-bit signed integer since the UNIX epoch (2017-01-01T00:00:00Z).
// The zero-value is a nanosecond and time zone neutral. Time zone neutral can be
// considered UTC without having "UTC" as a time zone.
type TimestampType struct {
	Unit     TimeUnit
	TimeZone string

	loc *time.Location
}

func (*TimestampType) ID() Type     { return TIMESTAMP }
func (*TimestampType) Name() string { return "timestamp" }
func (t *TimestampType) String() string {
	switch len(t.TimeZone) {
	case 0:
		return "timestamp[" + t.Unit.String() + "]"
	default:
		return "timestamp[" + t.Unit.String() + ", tz=" + t.TimeZone + "]"
	}
}

func (t *TimestampType) Fingerprint() string {
	return fmt.Sprintf("%s%d:%s", typeFingerprint(t)+string(timeUnitFingerprint(t.Unit)), len(t.TimeZone), t.TimeZone)
}

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (*TimestampType) BitWidth() int { return 64 }

func (TimestampType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(TimestampSizeBytes)}}
}

func (t *TimestampType) TimeUnit() TimeUnit { return t.Unit }

// ClearCachedLocation clears the cached time.Location object in the type.
// This should be called if you change the value of the TimeZone after having
// potentially called GetZone.
func (t *TimestampType) ClearCachedLocation() {
	t.loc = nil
}

// GetZone returns a *time.Location that represents the current TimeZone member
// of the TimestampType. If it is "", "UTC", or "utc", you'll get time.UTC.
// Otherwise it must either be a valid tzdata string such as "America/New_York"
// or of the format +HH:MM or -HH:MM indicating an absolute offset.
//
// The location object will be cached in the TimestampType for subsequent calls
// so if you change the value of TimeZone after calling this, make sure to call
// ClearCachedLocation.
func (t *TimestampType) GetZone() (*time.Location, error) {
	if t.loc != nil {
		return t.loc, nil
	}

	// the TimeZone string is allowed to be either a valid tzdata string
	// such as "America/New_York" or an absolute offset of the form -XX:XX
	// or +XX:XX
	//
	// As such we have two methods we can try, first we'll try LoadLocation
	// and if that fails, we'll test for an absolute offset.
	if t.TimeZone == "" || t.TimeZone == "UTC" || t.TimeZone == "utc" {
		t.loc = time.UTC
		return time.UTC, nil
	}

	if loc, err := time.LoadLocation(t.TimeZone); err == nil {
		t.loc = loc
		return t.loc, err
	}

	// at this point we know that the timezone isn't empty, and didn't match
	// anything in the tzdata names. So either it's an absolute offset
	// or it's invalid.
	timetz, err := time.Parse("-07:00", t.TimeZone)
	if err != nil {
		return time.UTC, fmt.Errorf("could not find timezone location for '%s'", t.TimeZone)
	}

	_, offset := timetz.Zone()
	t.loc = time.FixedZone(t.TimeZone, offset)
	return t.loc, nil
}

// GetToTimeFunc returns a function for converting an arrow.Timestamp value into a
// time.Time object with proper TimeZone and precision. If the TimeZone is invalid
// this will return an error. It calls GetZone to get the timezone for consistency.
func (t *TimestampType) GetToTimeFunc() (func(Timestamp) time.Time, error) {
	tz, err := t.GetZone()
	if err != nil {
		return nil, err
	}

	switch t.Unit {
	case Second:
		return func(v Timestamp) time.Time { return time.Unix(int64(v), 0).In(tz) }, nil
	case Millisecond:
		factor := int64(time.Second / time.Millisecond)
		return func(v Timestamp) time.Time {
			return time.Unix(int64(v)/factor, (int64(v)%factor)*int64(time.Millisecond)).In(tz)
		}, nil
	case Microsecond:
		factor := int64(time.Second / time.Microsecond)
		return func(v Timestamp) time.Time {
			return time.Unix(int64(v)/factor, (int64(v)%factor)*int64(time.Microsecond)).In(tz)
		}, nil
	case Nanosecond:
		return func(v Timestamp) time.Time { return time.Unix(0, int64(v)).In(tz) }, nil
	}
	return nil, fmt.Errorf("invalid timestamp unit: %s", t.Unit)
}

// Time32Type is encoded as a 32-bit signed integer, representing either seconds or milliseconds since midnight.
type Time32Type struct {
	Unit TimeUnit
}

func (*Time32Type) ID() Type         { return TIME32 }
func (*Time32Type) Name() string     { return "time32" }
func (*Time32Type) BitWidth() int    { return 32 }
func (t *Time32Type) String() string { return "time32[" + t.Unit.String() + "]" }
func (t *Time32Type) Fingerprint() string {
	return typeFingerprint(t) + string(timeUnitFingerprint(t.Unit))
}

func (Time32Type) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(Time32SizeBytes)}}
}

func (t *Time32Type) TimeUnit() TimeUnit { return t.Unit }

// Time64Type is encoded as a 64-bit signed integer, representing either microseconds or nanoseconds since midnight.
type Time64Type struct {
	Unit TimeUnit
}

func (*Time64Type) ID() Type         { return TIME64 }
func (*Time64Type) Name() string     { return "time64" }
func (*Time64Type) BitWidth() int    { return 64 }
func (t *Time64Type) String() string { return "time64[" + t.Unit.String() + "]" }
func (t *Time64Type) Fingerprint() string {
	return typeFingerprint(t) + string(timeUnitFingerprint(t.Unit))
}

func (Time64Type) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(Time64SizeBytes)}}
}

func (t *Time64Type) TimeUnit() TimeUnit { return t.Unit }

// DurationType is encoded as a 64-bit signed integer, representing an amount
// of elapsed time without any relation to a calendar artifact.
type DurationType struct {
	Unit TimeUnit
}

func (*DurationType) ID() Type         { return DURATION }
func (*DurationType) Name() string     { return "duration" }
func (*DurationType) BitWidth() int    { return 64 }
func (t *DurationType) String() string { return "duration[" + t.Unit.String() + "]" }
func (t *DurationType) Fingerprint() string {
	return typeFingerprint(t) + string(timeUnitFingerprint(t.Unit))
}

func (DurationType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(DurationSizeBytes)}}
}

func (t *DurationType) TimeUnit() TimeUnit { return t.Unit }

// Float16Type represents a floating point value encoded with a 16-bit precision.
type Float16Type struct{}

func (t *Float16Type) ID() Type            { return FLOAT16 }
func (t *Float16Type) Name() string        { return "float16" }
func (t *Float16Type) String() string      { return "float16" }
func (t *Float16Type) Fingerprint() string { return typeFingerprint(t) }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *Float16Type) BitWidth() int { return 16 }

func (Float16Type) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(Float16SizeBytes)}}
}

// Decimal128Type represents a fixed-size 128-bit decimal type.
type Decimal128Type struct {
	Precision int32
	Scale     int32
}

func (*Decimal128Type) ID() Type      { return DECIMAL128 }
func (*Decimal128Type) Name() string  { return "decimal" }
func (*Decimal128Type) BitWidth() int { return 128 }
func (t *Decimal128Type) String() string {
	return fmt.Sprintf("%s(%d, %d)", t.Name(), t.Precision, t.Scale)
}
func (t *Decimal128Type) Fingerprint() string {
	return fmt.Sprintf("%s[%d,%d,%d]", typeFingerprint(t), t.BitWidth(), t.Precision, t.Scale)
}

func (Decimal128Type) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(Decimal128SizeBytes)}}
}

// Decimal256Type represents a fixed-size 256-bit decimal type.
type Decimal256Type struct {
	Precision int32
	Scale     int32
}

func (*Decimal256Type) ID() Type      { return DECIMAL256 }
func (*Decimal256Type) Name() string  { return "decimal256" }
func (*Decimal256Type) BitWidth() int { return 256 }
func (t *Decimal256Type) String() string {
	return fmt.Sprintf("%s(%d, %d)", t.Name(), t.Precision, t.Scale)
}
func (t *Decimal256Type) Fingerprint() string {
	return fmt.Sprintf("%s[%d,%d,%d]", typeFingerprint(t), t.BitWidth(), t.Precision, t.Scale)
}

func (Decimal256Type) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(Decimal256SizeBytes)}}
}

// MonthInterval represents a number of months.
type MonthInterval int32

func (m *MonthInterval) UnmarshalJSON(data []byte) error {
	var val struct {
		Months int32 `json:"months"`
	}
	if err := json.Unmarshal(data, &val); err != nil {
		return err
	}

	*m = MonthInterval(val.Months)
	return nil
}

func (m MonthInterval) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Months int32 `json:"months"`
	}{int32(m)})
}

// MonthIntervalType is encoded as a 32-bit signed integer,
// representing a number of months.
type MonthIntervalType struct{}

func (*MonthIntervalType) ID() Type            { return INTERVAL_MONTHS }
func (*MonthIntervalType) Name() string        { return "month_interval" }
func (*MonthIntervalType) String() string      { return "month_interval" }
func (*MonthIntervalType) Fingerprint() string { return typeIDFingerprint(INTERVAL_MONTHS) + "M" }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *MonthIntervalType) BitWidth() int { return 32 }

func (MonthIntervalType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(MonthIntervalSizeBytes)}}
}

// DayTimeInterval represents a number of days and milliseconds (fraction of day).
type DayTimeInterval struct {
	Days         int32 `json:"days"`
	Milliseconds int32 `json:"milliseconds"`
}

// DayTimeIntervalType is encoded as a pair of 32-bit signed integer,
// representing a number of days and milliseconds (fraction of day).
type DayTimeIntervalType struct{}

func (*DayTimeIntervalType) ID() Type            { return INTERVAL_DAY_TIME }
func (*DayTimeIntervalType) Name() string        { return "day_time_interval" }
func (*DayTimeIntervalType) String() string      { return "day_time_interval" }
func (*DayTimeIntervalType) Fingerprint() string { return typeIDFingerprint(INTERVAL_DAY_TIME) + "d" }

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (t *DayTimeIntervalType) BitWidth() int { return 64 }

func (DayTimeIntervalType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(DayTimeIntervalSizeBytes)}}
}

// MonthDayNanoInterval represents a number of months, days and nanoseconds (fraction of day).
type MonthDayNanoInterval struct {
	Months      int32 `json:"months"`
	Days        int32 `json:"days"`
	Nanoseconds int64 `json:"nanoseconds"`
}

// MonthDayNanoIntervalType is encoded as two signed 32-bit integers representing
// a number of months and a number of days, followed by a 64-bit integer representing
// the number of nanoseconds since midnight for fractions of a day.
type MonthDayNanoIntervalType struct{}

func (*MonthDayNanoIntervalType) ID() Type       { return INTERVAL_MONTH_DAY_NANO }
func (*MonthDayNanoIntervalType) Name() string   { return "month_day_nano_interval" }
func (*MonthDayNanoIntervalType) String() string { return "month_day_nano_interval" }
func (*MonthDayNanoIntervalType) Fingerprint() string {
	return typeIDFingerprint(INTERVAL_MONTH_DAY_NANO) + "N"
}

// BitWidth returns the number of bits required to store a single element of this data type in memory.
func (*MonthDayNanoIntervalType) BitWidth() int { return 128 }

func (MonthDayNanoIntervalType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(), SpecFixedWidth(MonthDayNanoIntervalSizeBytes)}}
}

type op int8

const (
	convDIVIDE = iota
	convMULTIPLY
)

var timestampConversion = [...][4]struct {
	op     op
	factor int64
}{
	Nanosecond: {
		Nanosecond:  {convMULTIPLY, int64(time.Nanosecond)},
		Microsecond: {convDIVIDE, int64(time.Microsecond)},
		Millisecond: {convDIVIDE, int64(time.Millisecond)},
		Second:      {convDIVIDE, int64(time.Second)},
	},
	Microsecond: {
		Nanosecond:  {convMULTIPLY, int64(time.Microsecond)},
		Microsecond: {convMULTIPLY, 1},
		Millisecond: {convDIVIDE, int64(time.Millisecond / time.Microsecond)},
		Second:      {convDIVIDE, int64(time.Second / time.Microsecond)},
	},
	Millisecond: {
		Nanosecond:  {convMULTIPLY, int64(time.Millisecond)},
		Microsecond: {convMULTIPLY, int64(time.Millisecond / time.Microsecond)},
		Millisecond: {convMULTIPLY, 1},
		Second:      {convDIVIDE, int64(time.Second / time.Millisecond)},
	},
	Second: {
		Nanosecond:  {convMULTIPLY, int64(time.Second)},
		Microsecond: {convMULTIPLY, int64(time.Second / time.Microsecond)},
		Millisecond: {convMULTIPLY, int64(time.Second / time.Millisecond)},
		Second:      {convMULTIPLY, 1},
	},
}

func ConvertTimestampValue(in, out TimeUnit, value int64) int64 {
	conv := timestampConversion[int(in)][int(out)]
	switch conv.op {
	case convMULTIPLY:
		return value * conv.factor
	case convDIVIDE:
		return value / conv.factor
	}

	return 0
}

// DictionaryType represents categorical or dictionary-encoded in-memory data
// It contains a dictionary-encoded value type (any type) and an index type
// (any integer type).
type DictionaryType struct {
	IndexType DataType
	ValueType DataType
	Ordered   bool
}

func (*DictionaryType) ID() Type        { return DICTIONARY }
func (*DictionaryType) Name() string    { return "dictionary" }
func (d *DictionaryType) BitWidth() int { return d.IndexType.(FixedWidthDataType).BitWidth() }
func (d *DictionaryType) String() string {
	return fmt.Sprintf("%s<values=%s, indices=%s, ordered=%t>",
		d.Name(), d.ValueType, d.IndexType, d.Ordered)
}
func (d *DictionaryType) Fingerprint() string {
	indexFingerprint := d.IndexType.Fingerprint()
	valueFingerprint := d.ValueType.Fingerprint()
	ordered := "1"
	if !d.Ordered {
		ordered = "0"
	}

	if len(valueFingerprint) > 0 {
		return typeFingerprint(d) + indexFingerprint + valueFingerprint + ordered
	}
	return ordered
}

func (d *DictionaryType) Layout() DataTypeLayout {
	layout := d.IndexType.Layout()
	layout.HasDict = true
	return layout
}

var (
	FixedWidthTypes = struct {
		Boolean              FixedWidthDataType
		Date32               FixedWidthDataType
		Date64               FixedWidthDataType
		DayTimeInterval      FixedWidthDataType
		Duration_s           FixedWidthDataType
		Duration_ms          FixedWidthDataType
		Duration_us          FixedWidthDataType
		Duration_ns          FixedWidthDataType
		Float16              FixedWidthDataType
		MonthInterval        FixedWidthDataType
		Time32s              FixedWidthDataType
		Time32ms             FixedWidthDataType
		Time64us             FixedWidthDataType
		Time64ns             FixedWidthDataType
		Timestamp_s          FixedWidthDataType
		Timestamp_ms         FixedWidthDataType
		Timestamp_us         FixedWidthDataType
		Timestamp_ns         FixedWidthDataType
		MonthDayNanoInterval FixedWidthDataType
	}{
		Boolean:              &BooleanType{},
		Date32:               &Date32Type{},
		Date64:               &Date64Type{},
		DayTimeInterval:      &DayTimeIntervalType{},
		Duration_s:           &DurationType{Unit: Second},
		Duration_ms:          &DurationType{Unit: Millisecond},
		Duration_us:          &DurationType{Unit: Microsecond},
		Duration_ns:          &DurationType{Unit: Nanosecond},
		Float16:              &Float16Type{},
		MonthInterval:        &MonthIntervalType{},
		Time32s:              &Time32Type{Unit: Second},
		Time32ms:             &Time32Type{Unit: Millisecond},
		Time64us:             &Time64Type{Unit: Microsecond},
		Time64ns:             &Time64Type{Unit: Nanosecond},
		Timestamp_s:          &TimestampType{Unit: Second, TimeZone: "UTC"},
		Timestamp_ms:         &TimestampType{Unit: Millisecond, TimeZone: "UTC"},
		Timestamp_us:         &TimestampType{Unit: Microsecond, TimeZone: "UTC"},
		Timestamp_ns:         &TimestampType{Unit: Nanosecond, TimeZone: "UTC"},
		MonthDayNanoInterval: &MonthDayNanoIntervalType{},
	}

	_ FixedWidthDataType = (*FixedSizeBinaryType)(nil)
)
