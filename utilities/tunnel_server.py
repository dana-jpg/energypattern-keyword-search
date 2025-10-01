import os
# import wexpect
import dotenv

dotenv.load_dotenv()

password = os.getenv("SERVER_PASSWORD")
#
#
# # SSH command
# command = "ssh -L 11434:localhost:11434 dmitry@145.108.224.54"
#
# try:
#     # Start the process
#     child = wexpect.spawn(command)
#
#     # Expect the password prompt
#     child.expect("dmitry@145.108.224.54's password:")
#
#     # Send the password
#     child.sendline(password)
#
#     # Wait for the command to complete
#     child.interact()  # This will give control back to the user
# except Exception as e:
#     print(f"An error occurred: {e}")

import subprocess

# SSH command using plink
command = [
    "plink",
    "-ssh",
    "-L", "11434:localhost:11434",
    "dmitry@145.108.224.54",
    "-pw", password  # Pass the password directly (not recommended for security)
]

# Execute the command
try:
    print("Starting SSH tunnel with plink...")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # Print output
    print("STDOUT:", stdout.decode())
    print("STDERR:", stderr.decode())
except Exception as e:
    print(f"An error occurred: {e}")