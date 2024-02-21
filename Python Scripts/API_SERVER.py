from flask import Flask, request, jsonify
import hashlib

app = Flask(__name__)

auth_token = '823jkhvbasj52nS'

users = [ ] #Everytime there will be server reload the users will be deleted because there is no database attached.


def authenticate_request():
    auth_header = request.headers.get('AUTHTOKEN') #AUTHTOKEN Header
    if auth_header is None or auth_header != auth_token:
        return False
    return True

def get_user_by_id(user_id):
    for user in users:
        if user['user_id'] == user_id:
            return user
    return None

@app.route('/')
def landing_page():
    return "Welcome to the API Server Demo. Use authenticated Token to interact."

#GET USER DETAILS
@app.route('/v1/user', methods=['GET'])
def get_users():
    if not authenticate_request():
        return jsonify({"error": "Unauthorized"}), 401

    user_id = request.args.get('user_id')
    if user_id:
        user = get_user_by_id(int(user_id))
        if user:
            return jsonify({"users": [user], "errors": []})
        else:
            return jsonify({"users": [], "errors": ["User not found"]})
    else:
        return jsonify({"users": users, "errors": []})

#CREATE USER
@app.route('/v1/user', methods=['POST'])
def create_user():
    if not authenticate_request():
        return jsonify({"error": "Unauthorized"}), 401 #Unauthorized error 

    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    user_id = len(users) + 1
    new_user = {
        "user_id": user_id,
        "username": username,
        "password": hashlib.sha1((username + password).encode()).hexdigest()
    }
    users.append(new_user)
    return jsonify({"users": [new_user], "errors": []})

#UPDATE USER
@app.route('/v1/user', methods=['PUT'])
def update_user():
    if not authenticate_request():
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    user_id = int(request.args.get('user_id'))
    username = data.get('username')
    password = data.get('password')

    user = get_user_by_id(user_id)
    if user:
        user['username'] = username
        user['password'] = hashlib.sha1((username + password).encode()).hexdigest()
        return jsonify({"users": [user], "errors": []})
    else:
        return jsonify({"users": [], "errors": ["User not found"]})

#DELETE USER
@app.route('/v1/user', methods=['DELETE'])
def delete_user():
    if not authenticate_request():
        return jsonify({"error": "Unauthorized"}), 401

    user_id = int(request.args.get('user_id'))
    global users
    users = [user for user in users if user['user_id'] != user_id]
    return jsonify({"users": [], "errors": []})

if __name__ == '__main__':
    app.run(debug=True)
