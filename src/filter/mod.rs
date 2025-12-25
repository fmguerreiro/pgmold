use glob::Pattern;

pub struct Filter {
    include: Vec<Pattern>,
    exclude: Vec<Pattern>,
}

impl Filter {
    pub fn new(include: &[String], exclude: &[String]) -> Result<Self, glob::PatternError> {
        let include_patterns = include
            .iter()
            .map(|s| Pattern::new(s))
            .collect::<Result<Vec<_>, _>>()?;

        let exclude_patterns = exclude
            .iter()
            .map(|s| Pattern::new(s))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Filter {
            include: include_patterns,
            exclude: exclude_patterns,
        })
    }

    pub fn should_include(&self, name: &str) -> bool {
        if !self.exclude.is_empty() {
            for pattern in &self.exclude {
                if pattern.matches(name) {
                    return false;
                }
            }
        }

        if !self.include.is_empty() {
            for pattern in &self.include {
                if pattern.matches(name) {
                    return true;
                }
            }
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_filters_includes_everything() {
        let filter = Filter::new(&[], &[]).unwrap();
        assert!(filter.should_include("anything"));
    }

    #[test]
    fn exclude_underscore_prefix() {
        let filter = Filter::new(&[], &["_*".to_string()]).unwrap();
        assert!(!filter.should_include("_add"));
        assert!(filter.should_include("api_change"));
    }

    #[test]
    fn include_pattern_filters() {
        let include = vec!["api_*".to_string()];
        let filter = Filter::new(&include, &[]).unwrap();
        assert!(filter.should_include("api_user"));
        assert!(!filter.should_include("st_distance"));
    }

    #[test]
    fn exclude_takes_precedence() {
        let include = vec!["api_*".to_string()];
        let exclude = vec!["*_test".to_string()];
        let filter = Filter::new(&include, &exclude).unwrap();
        assert!(!filter.should_include("api_test"));
    }

    #[test]
    fn qualified_name_patterns() {
        let include = vec!["public.api_*".to_string()];
        let filter = Filter::new(&include, &[]).unwrap();
        assert!(filter.should_include("public.api_user"));
        assert!(!filter.should_include("auth.api_user"));
    }

    #[test]
    fn question_mark_matches_single_char() {
        let include = vec!["api_?".to_string()];
        let filter = Filter::new(&include, &[]).unwrap();
        assert!(filter.should_include("api_a"));
        assert!(!filter.should_include("api_ab"));
    }

    #[test]
    fn invalid_pattern_returns_error() {
        let invalid_include = vec!["[invalid".to_string()];
        assert!(Filter::new(&invalid_include, &[]).is_err());

        let invalid_exclude = vec!["[invalid".to_string()];
        assert!(Filter::new(&[], &invalid_exclude).is_err());
    }
}
