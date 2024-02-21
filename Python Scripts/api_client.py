import argparse
import requests
import json

api_url = 'http://127.0.0.1:5000/v1/user'
api_key = '823jkhvbasj52nS'

headers = {
    'AUTHTOKEN': api_key,
    'Content-Type': 'application/json',
}
def get_users():
    response = requests.get(api_url, headers=headers)
    handle_response(response)

def create_user(username, password):
    post_body = {"username": username, "password": password}
    response = requests.post(api_url, json=post_body, headers=headers)
    handle_response(response)

def update_user(user_id, username, password):
    put_body = {"username": username, "password": password}
    put_endpoint = f'{api_url}/{user_id}'
    response = requests.put(put_endpoint, json=put_body, headers=headers)
    handle_response(response)

def delete_user(user_id):
    delete_endpoint = f'{api_url}?user_id={user_id}'
    response = requests.delete(delete_endpoint, headers=headers)
    handle_response(response)

def handle_response(response):
    if response.status_code == 200:
        result = response.json()
        print(json.dumps(result, indent=2))
    else:
        print(f"Error: {response.status_code}, {response.text}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--get", action="store_true", help="Retrieve all users")
    parser.add_argument("-c", "--create", nargs=2, metavar=("username", "password"), help="Create a new user")
    parser.add_argument("-u", "--update", nargs=3, metavar=("user_id", "username", "password"), help="Update a user")
    parser.add_argument("-d", "--delete", metavar="user_id", help="Delete a user")
    args = parser.parse_args()

    if args.get:
        get_users()
    elif args.create:
        create_user(args.create[0], args.create[1])
    elif args.update:
        update_user(args.update[0], args.update[1], args.update[2])
    elif args.delete:
        delete_user(args.delete)
    else:
        parser.print_help()
        print("No valid action specified from GET (-g), POST (-c), PUT (-u), DELETE (-d)")

if __name__ == "__main__":
    main()
