#[derive(Debug, Clone)]
pub struct Line {
    data: Vec<u8>,
    series_name_len: u8,
    tags: Vec<KV>,
    fields: Vec<KV>,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
struct KV {
    start: u16,
    end: u16,
    divider: u16,
}

impl KV {
    fn new() -> Self {
        KV {
            start: 0,
            end: 0,
            divider: 0,
        }
    }

    fn new_kv_from(position: u16) -> Self {
        KV {
            start: position,
            end: 0,
            divider: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.start != 0 && self.end != 0 && self.divider != 0
    }
}

struct LineFieldIter<'a> {
    line: &'a Line,
    curr_field: usize,
}

impl LineFieldIter<'_> {
    fn new<'a>(line: &'a Line) -> LineFieldIter<'a> {
        LineFieldIter {
            line: line,
            curr_field: 0,
        }
    }
}

impl<'a> Iterator for LineFieldIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<(&'a str, &'a str)> {
        if self.curr_field >= self.line.fields.len() {
            return None;
        } else {
            let kv: &KV = unsafe { self.line.fields.get_unchecked(self.curr_field) };
            let key = unsafe {
                std::str::from_utf8_unchecked(
                    &self.line.data[kv.start as usize..kv.divider as usize],
                )
            };
            let value = unsafe {
                std::str::from_utf8_unchecked(
                    &self.line.data[(kv.divider + 1) as usize..kv.end as usize],
                )
            };
            self.curr_field += 1;
            return Some((key, value));
        }
    }
}

const COMMA: u8 = ',' as u8;
const EQUALS: u8 = '=' as u8;
const SPACE: u8 = ' ' as u8;

impl Line {
    fn parse_keyvalues(line: &[u8], start: usize, tags: &mut Vec<KV>) -> Result<usize, ()> {
        let mut position = start;
        let mut current_tag = KV::new_kv_from(position as u16);
        while position < line.len() && line[position] != SPACE {
            match line[position] {
                EQUALS => {
                    current_tag.divider = position as u16;
                }
                COMMA => {
                    current_tag.end = position as u16;
                    if !current_tag.is_complete() {
                        return Err(());
                    }
                    tags.push(current_tag);
                    current_tag = KV::new_kv_from((position + 1) as u16);
                }
                _ => { /* do nothing */ }
            }
            position += 1;
        }

        current_tag.end = position as u16;
        if !current_tag.is_complete() {
            return Err(());
        }
        tags.push(current_tag);
        Ok(position)
    }

    pub fn timeseries_name(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.data[0..self.series_name_len as usize]) }
    }

    fn kvs_to_str(&self, kvs: &[KV]) -> Vec<(&str, &str)> {
        let mut result = Vec::with_capacity(kvs.len());
        for kv in kvs.iter() {
            let key = unsafe {
                std::str::from_utf8_unchecked(&self.data[kv.start as usize..kv.divider as usize])
            };
            let value = unsafe {
                std::str::from_utf8_unchecked(
                    &self.data[(kv.divider + 1) as usize..kv.end as usize],
                )
            };
            result.push((key, value))
        }
        result
    }
    pub fn tags(&self) -> Vec<(&str, &str)> {
        self.kvs_to_str(&self.tags)
    }

    pub fn fields(&self) -> Vec<(&str, &str)> {
        self.kvs_to_str(&self.fields)
    }

    pub fn fields_iter(&self) -> impl Iterator<Item = (&str, &str)> {
        LineFieldIter::new(self)
    }

    pub fn parse(line: &[u8]) -> Option<Line> {
        let size = line.len();
        let mut data = Vec::from(line);
        let mut series_name_len = 0;
        let mut position = 0 as usize;

        while position < size && line[position] != COMMA && line[position] != SPACE {
            if line[position] == EQUALS {
                return None;
            }
            position += 1;
        }

        if position == size {
            return None;
        }
        series_name_len = position;
        let mut tags = Vec::new();
        if line[position] == COMMA {
            position += 1;
            match Line::parse_keyvalues(&line, position, &mut tags) {
                Ok(new_position) => position = new_position,
                Err(()) => return None,
            }
        }

        if position == size || line[position] != SPACE {
            return None;
        }

        position += 1;

        let mut fields = Vec::new();

        match Line::parse_keyvalues(&line, position, &mut fields) {
            Ok(new_position) => position = new_position,
            Err(()) => return None,
        }

        if position == size || line[position] != SPACE {
            return None;
        }

        position += 1;
        let mut timestamp = 0 as u64;

        let before_ts = position;
        while position < size && line[position] >= 48 && line[position] <= 57 {
            timestamp = timestamp * 10 + (line[position] - 48) as u64;
            position += 1;
        }

        if before_ts == position {
            return None;
        }

        Some(Line {
            data: data,
            series_name_len: series_name_len as u8,
            tags: tags,
            fields: fields,
            timestamp: timestamp,
        })
    }
}

#[cfg(test)]
mod test {
    use claim::assert_none;

    use super::*;

    #[test]
    fn simple_test() {
        let str =
            "weather,location=us-midwest,country=us temperature=82,humidity=75 1465839830100400200";
        let line = Line::parse(str.as_bytes()).expect("should exist");
        assert_eq!("weather", line.timeseries_name());

        assert_eq!(1465839830100400200, line.timestamp);

        assert_eq!(
            vec![("location", "us-midwest"), ("country", "us")],
            line.tags()
        );
        assert_eq!(
            vec![("temperature", "82"), ("humidity", "75")],
            line.fields()
        );
    }

    #[test]
    fn no_tags() {
        let str = "weather temperature=82,humidity=75 1465839830100400200";
        let line = Line::parse(str.as_bytes()).expect("should exist");
        assert_eq!("weather", line.timeseries_name());

        assert_eq!(1465839830100400200, line.timestamp);

        assert_eq!(Vec::<(&str, &str)>::new(), line.tags());
        assert_eq!(
            vec![("temperature", "82"), ("humidity", "75")],
            line.fields()
        );
    }

    #[test]
    fn timestamp_is_manadtory() {
        let str = "weather temperature=82,humidity=75";
        let line = Line::parse(str.as_bytes());
        assert_none!(line);
    }

    #[test]
    fn series_name_is_mandatory() {
        let str = "temperature=82,humidity=75 1465839830100400200";
        let line = Line::parse(str.as_bytes());
        assert_none!(line);
    }

    #[test]
    fn at_least_one_field_is_required() {
        let str = "weather 1465839830100400200";
        let line = Line::parse(str.as_bytes());
        assert_none!(line);
    }

    #[test]
    fn test_field_iterator() {
        let str =
            "weather,location=us-midwest,country=us temperature=82,humidity=75 1465839830100400200";
        let line = Line::parse(str.as_bytes()).expect("should exist");
        let fields_from_iter: Vec<(&str, &str)> = line.fields_iter().collect();
        assert_eq!(
            vec![("temperature", "82"), ("humidity", "75")],
            fields_from_iter
        );
    }
}
