"""
Content quality validation for scraped pages
"""
import re
from typing import Dict, List
from model import PageData


class ContentQualityValidator:
    """Validate the quality of extracted content"""
    
    def __init__(self):
        # Common navigation/menu keywords that shouldn't be in main content
        self.nav_keywords = [
            'hauptnavigation', 'navigation', 'breadcrumb', 'hauptmenü',
            'quick links', 'sitemap', 'kontakt', 'impressum', 'datenschutz',
            'login', 'logout', 'mein konto', 'warenkorb', 'suche'
        ]
        
        # Words that indicate quality content
        self.content_indicators = [
            'forschung', 'wissenschaft', 'studium', 'lehre', 'universität',
            'fakultät', 'institut', 'professor', 'dissertation', 'projekt'
        ]
    
    def validate_page_content(self, page_data: PageData) -> Dict:
        """Validate the quality of a scraped page"""
        text = page_data.text.lower()
        
        validation_result = {
            'url': page_data.url,
            'is_valid': True,
            'issues': [],
            'quality_score': 0,
            'word_count': len(text.split()),
            'has_navigation_artifacts': False,
            'content_density': 0
        }
        
        # Check minimum content length
        if len(text) < 100:
            validation_result['issues'].append('Content too short (< 100 characters)')
            validation_result['is_valid'] = False
        
        # Check for navigation artifacts
        nav_artifact_count = sum(1 for keyword in self.nav_keywords if keyword in text)
        if nav_artifact_count > 2:
            validation_result['has_navigation_artifacts'] = True
            validation_result['issues'].append(f'Contains {nav_artifact_count} navigation keywords')
        
        # Calculate content quality score
        content_score = sum(1 for indicator in self.content_indicators if indicator in text)
        validation_result['quality_score'] = content_score
        
        # Calculate content density (meaningful text vs total text)
        meaningful_sentences = len([s for s in text.split('.') if len(s.strip()) > 20])
        total_sentences = len(text.split('.'))
        if total_sentences > 0:
            validation_result['content_density'] = meaningful_sentences / total_sentences
        
        # Overall validation
        if (validation_result['quality_score'] < 2 and 
            validation_result['content_density'] < 0.5):
            validation_result['is_valid'] = False
            validation_result['issues'].append('Low content quality detected')
        
        return validation_result
    
    def validate_batch(self, pages: List[PageData]) -> Dict:
        """Validate a batch of pages and provide summary statistics"""
        results = [self.validate_page_content(page) for page in pages]
        
        valid_pages = [r for r in results if r['is_valid']]
        invalid_pages = [r for r in results if not r['is_valid']]
        
        summary = {
            'total_pages': len(pages),
            'valid_pages': len(valid_pages),
            'invalid_pages': len(invalid_pages),
            'validation_rate': len(valid_pages) / len(pages) if pages else 0,
            'average_word_count': sum(r['word_count'] for r in results) / len(results) if results else 0,
            'average_quality_score': sum(r['quality_score'] for r in results) / len(results) if results else 0,
            'pages_with_nav_artifacts': len([r for r in results if r['has_navigation_artifacts']]),
            'common_issues': self._get_common_issues(results)
        }
        
        return {
            'summary': summary,
            'detailed_results': results,
            'invalid_pages': invalid_pages
        }
    
    def _get_common_issues(self, results: List[Dict]) -> Dict[str, int]:
        """Get frequency of common issues"""
        issue_counts = {}
        for result in results:
            for issue in result['issues']:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
        return dict(sorted(issue_counts.items(), key=lambda x: x[1], reverse=True))


def print_validation_report(validation_result: Dict) -> None:
    """Print a formatted validation report"""
    summary = validation_result['summary']
    
    print(f"\n{'='*60}")
    print(f"CONTENT QUALITY VALIDATION REPORT")
    print(f"{'='*60}")
    print(f"Total Pages Processed: {summary['total_pages']}")
    print(f"Valid Pages: {summary['valid_pages']}")
    print(f"Invalid Pages: {summary['invalid_pages']}")
    print(f"Validation Rate: {summary['validation_rate']:.2%}")
    print(f"Average Word Count: {summary['average_word_count']:.0f}")
    print(f"Average Quality Score: {summary['average_quality_score']:.1f}")
    print(f"Pages with Navigation Artifacts: {summary['pages_with_nav_artifacts']}")
    
    if summary['common_issues']:
        print(f"\nMost Common Issues:")
        for issue, count in list(summary['common_issues'].items())[:5]:
            print(f"  • {issue}: {count} pages")
    
    if validation_result['invalid_pages']:
        print(f"\nSample Invalid Pages:")
        for page in validation_result['invalid_pages'][:3]:
            print(f"  • {page['url']}")
            print(f"    Issues: {', '.join(page['issues'])}")
            print(f"    Word Count: {page['word_count']}, Quality Score: {page['quality_score']}")
