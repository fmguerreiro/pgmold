use glob::Pattern;

pub fn should_include_object(name: &str, include: &[String], exclude: &[String]) -> bool {
    if !exclude.is_empty() {
        for pattern_str in exclude {
            if let Ok(pattern) = Pattern::new(pattern_str) {
                if pattern.matches(name) {
                    return false;
                }
            }
        }
    }

    if !include.is_empty() {
        for pattern_str in include {
            if let Ok(pattern) = Pattern::new(pattern_str) {
                if pattern.matches(name) {
                    return true;
                }
            }
        }
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_filters_includes_everything() {
        assert!(should_include_object("anything", &[], &[]));
    }

    #[test]
    fn exclude_underscore_prefix() {
        assert!(!should_include_object("_add", &[], &["_*".to_string()]));
        assert!(should_include_object("api_change", &[], &["_*".to_string()]));
    }

    #[test]
    fn include_pattern_filters() {
        let include = vec!["api_*".to_string()];
        assert!(should_include_object("api_user", &include, &[]));
        assert!(!should_include_object("st_distance", &include, &[]));
    }

    #[test]
    fn exclude_takes_precedence() {
        let include = vec!["api_*".to_string()];
        let exclude = vec!["*_test".to_string()];
        assert!(!should_include_object("api_test", &include, &exclude));
    }

    #[test]
    fn qualified_name_patterns() {
        let include = vec!["public.api_*".to_string()];
        assert!(should_include_object("public.api_user", &include, &[]));
        assert!(!should_include_object("auth.api_user", &include, &[]));
    }

    #[test]
    fn question_mark_matches_single_char() {
        let include = vec!["api_?".to_string()];
        assert!(should_include_object("api_a", &include, &[]));
        assert!(!should_include_object("api_ab", &include, &[]));
    }
}
