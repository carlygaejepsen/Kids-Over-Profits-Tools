from playwright.sync_api import sync_playwright

def test_playwright():
    print("Testing Playwright installation...")
    
    with sync_playwright() as p:
        # Launch browser
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Navigate to a test page
        page.goto('https://example.com')
        
        # Get page title
        title = page.title()
        print(f"Page title: {title}")
        
        # Close browser
        browser.close()
        
    print("Playwright test completed successfully!")

if __name__ == "__main__":
    test_playwright()