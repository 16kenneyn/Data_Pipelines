from helium import *
import time
# from selenium.webdriver.common.keys import Keys

# Start Chrome and navigate to the Redfin rental market data page
start_chrome('https://www.redfin.com/news/data-center/rental-market-data/')

# Wait for and click the 'National' button to open the search bar
wait_until(Button('National').exists, timeout_secs=30)
click('National')

# Wait for the search bar to appear (adjust the identifier if needed)
wait_until(Text('Region').exists, timeout_secs=30)  # Assuming 'Region' is near the search bar

# Get the WebDriver instance from Helium
driver = get_driver()

# Simulate pressing Tab once to move focus to the "All" checkbox
# You may need to adjust the number of Tab presses (e.g., 1 or 2) based on the page
driver.switch_to.active_element.send_keys(Keys.TAB)

# Simulate pressing Space to select the "All" checkbox
driver.switch_to.active_element.send_keys(Keys.SPACE)

# Wait for and click the 'Apply' button to confirm the selection
wait_until(Button('Apply').exists, timeout_secs=30)
click('Apply')

time.sleep(15)

# wait_until(Button('(All)').exists, timeout_secs=30)
click('(All)')
# click('Choose a format to download')
time.sleep(5)

# wait_until(Button('Download').exists, timeout_secs=30)
click('Download')
time.sleep(5)
click('Crosstab')
time.sleep(5)
driver.switch_to.active_element.send_keys(Keys.TAB)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.TAB)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.TAB)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.TAB)
time.sleep(3)

driver.switch_to.active_element.send_keys(Keys.ARROW_DOWN)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.SPACE)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.TAB)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.TAB)
time.sleep(3)
driver.switch_to.active_element.send_keys(Keys.SPACE)
time.sleep(3)
# Continue with your automation as needed...