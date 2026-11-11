namespace RocksDb.Managed.Options;

/// <summary>Port of OffpeakTimeInfo (options/offpeak_time_info.h).</summary>
internal readonly record struct OffpeakTimeInfo(bool IsNowOffpeak, int SecondsTillNextOffpeakStart);

/// <summary>
/// Port of OffpeakTimeOption (options/offpeak_time_info.h, options/offpeak_time_info.cc) and the time range parsing of
/// util/string_util.cc (ParseTimeStringToSeconds, TryParseTimeRangeString): DBOptions::daily_offpeak_time_utc
/// "HH:mm-HH:mm" in UTC, as seconds since midnight.
/// </summary>
internal sealed class OffpeakTimeOption
{
    private const int SecondsPerDay = 86400;
    private const int SecondsPerMinute = 60;

    public OffpeakTimeOption(string offpeakTimeString) => SetFromOffpeakTimeString(offpeakTimeString);

    public string DailyOffpeakTimeUtc { get; private set; } = "";
    public int DailyOffpeakStartTimeUtc { get; private set; }
    public int DailyOffpeakEndTimeUtc { get; private set; }

    /// <summary>SetFromOffpeakTimeString: keeps the previous window when the string does not parse.</summary>
    public void SetFromOffpeakTimeString(string offpeakTimeString)
    {
        if (TryParseTimeRange(offpeakTimeString, out int start, out int end))
        {
            DailyOffpeakStartTimeUtc = start;
            DailyOffpeakEndTimeUtc = end;
            DailyOffpeakTimeUtc = offpeakTimeString;
        }
    }

    /// <summary>GetOffpeakTimeInfo for <paramref name="currentTime"/> (seconds since the Unix epoch).</summary>
    public OffpeakTimeInfo GetOffpeakTimeInfo(long currentTime)
    {
        if (DailyOffpeakStartTimeUtc == DailyOffpeakEndTimeUtc) return default;
        int secondsSinceMidnight = (int)(currentTime % SecondsPerDay);
        int toNearestMinute = secondsSinceMidnight / SecondsPerMinute * SecondsPerMinute;
        bool isNowOffpeak = DailyOffpeakStartTimeUtc > DailyOffpeakEndTimeUtc
            // the offpeak duration spans overnight (i.e. 23:30 - 4:30 next day)
            ? DailyOffpeakStartTimeUtc <= toNearestMinute || toNearestMinute <= DailyOffpeakEndTimeUtc
            : DailyOffpeakStartTimeUtc <= toNearestMinute && toNearestMinute <= DailyOffpeakEndTimeUtc;
        int tillNext = secondsSinceMidnight < DailyOffpeakStartTimeUtc
            ? DailyOffpeakStartTimeUtc - secondsSinceMidnight
            : DailyOffpeakStartTimeUtc + SecondsPerDay - secondsSinceMidnight;
        return new OffpeakTimeInfo(isNowOffpeak, tillNext);
    }

    /// <summary>TryParseTimeRangeString: "" is the empty window (0, 0); otherwise exactly two "HH:mm" separated by '-'.</summary>
    public static bool TryParseTimeRange(string value, out int startTime, out int endTime)
    {
        startTime = 0;
        endTime = 0;
        if (value.Length == 0) return true;
        string[] split = value.Split('-');
        // StringSplit (util/string_util.cc) reads with std::getline, which yields no final empty piece: "a-b-" is two.
        int parts = split[^1].Length == 0 ? split.Length - 1 : split.Length;
        if (parts != 2) return false;
        startTime = ParseTimeStringToSeconds(split[0]);
        if (startTime < 0) return false;
        endTime = ParseTimeStringToSeconds(split[1]);
        return endTime >= 0;
    }

    /// <summary>
    /// ParseTimeStringToSeconds: <c>stream &gt;&gt; hours &gt;&gt; colon &gt;&gt; minutes</c> must consume the whole string
    /// with colon == ':', 0 &lt;= hours &lt;= 23, 0 &lt;= minutes &lt;= 59; returns -1 otherwise.
    /// </summary>
    private static int ParseTimeStringToSeconds(string value)
    {
        int pos = 0;
        if (!TryReadInt(value, ref pos, out int hours)) return -1;
        while (pos < value.Length && char.IsWhiteSpace(value[pos])) pos++;
        if (pos >= value.Length || value[pos] != ':') return -1;
        pos++;
        if (!TryReadInt(value, ref pos, out int minutes)) return -1;
        if (pos != value.Length) return -1;
        if (hours is < 0 or > 23 || minutes is < 0 or > 59) return -1;
        return hours * 3600 + minutes * 60;
    }

    /// <summary>operator&gt;&gt;(int): skips leading white space, optional sign, at least one digit.</summary>
    private static bool TryReadInt(string s, ref int pos, out int value)
    {
        value = 0;
        while (pos < s.Length && char.IsWhiteSpace(s[pos])) pos++;
        bool neg = false;
        if (pos < s.Length && s[pos] is '+' or '-')
        {
            neg = s[pos] == '-';
            pos++;
        }
        int start = pos;
        long v = 0;
        while (pos < s.Length && s[pos] is >= '0' and <= '9')
        {
            v = v * 10 + (s[pos] - '0');
            if (v > int.MaxValue) return false;
            pos++;
        }
        if (pos == start) return false;
        value = (int)(neg ? -v : v);
        return true;
    }
}
