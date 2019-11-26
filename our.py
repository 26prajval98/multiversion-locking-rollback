count = -1
mdb = dict()


def ts():
	global count
	count += 1
	return count


def generate_timestamps(t_o):
	t_s = dict()
	for i in t_o:
		t_s[i] = ts()
	return t_s


def create_mvdb(it):
	if it not in mdb.keys():
		mdb[it] = []


def read(it, tran_rts):
	create_mvdb(it)
	if len(mdb[it]) == 0:
		return -2
	else:
		if tran_rts >= mdb[it][-1]['w']:
			mdb[it][-1]['r'] = max(tran_rts, mdb[it][-1]['r'])
			# Successful
			return 0
	# Abort
	return -1


def write(it, trans):
	create_mvdb(it)
	if len(mdb[it]) == 0:
		temp = dict()
		temp['r'] = temp['w'] = tran_wts[1]
		temp['t'] = trans[0]
		mdb[it].append(temp)
		return 0
	else:
		if tran_wts >= mdb[it][-1]['r']:
			temp = dict()
			temp['r'] = temp['w'] = tran_wts[1]
			temp['t'] = trans[0]
			mdb[it].append(temp)
			# Successful
			return 0
	# Abort
	return -1


def rollback(it, tran_wts):
	rbacks = []
	# print("ROLL", tran_wts)
	while mdb[it][-1]['r'] > tran_wts:
		rbacks.append(mdb[it][-1]['t'])
		del mdb[it][-1]
	return rbacks


# R : 1
# W : 1
# C : 3

table = list(
[
	[(1, 1), (-1, -1), (0, 1), (-1, -1)],
	[(2, -1), (0, 1), (-1, -1), (-1, -1)],
	[(-1, -1), (-1, -1), (-1, -1), (1, 1)],
	[(-1, -1), (-1, -1), (1, 1), (2, -1)],
	[(-1, -1), (1, 1), (2, -1), (-1, -1)],
	[(-1, -1), (2, -1), (-1, -1), (-1, -1)],
])

transaction_orders = list([0, 1, 2, 3])
items = [1]
transaction_order = [i for i in range(len(transaction_orders))]
transaction_timestamps = generate_timestamps(transaction_order)

# ####################################################
# transaction_finish_order_wait = list([])
# Wait
# Transactions with lesser timestamps must finish first
# waiting_queue = []
# for t in transaction_order:
# 	for row in range(len(table)-1):
# 		if table[row][t][0] == 1:
# 			waiting_queue.append(t)
# 			break
#
# while len(waiting_queue):
# 	waiting_queue.pop(0)
# 	for t in transaction_order:
# 		for row in range(len(table)-1):
# 			if t not in waiting_queue and t not in transaction_finish_order_wait:
# 				operation = table[row][t][0]
# 				item = table[row][t][1]
# 				if operation == -1 or operation == 2:
# 					pass
# 				else:
# 					if operation == 0:
# 						read(item, transaction_timestamps[t])
# 					if operation == 1:
# 						res = write(item, [t, transaction_timestamps[t]])
# 					if table[row + 1][t][0] == 2 and t not in transaction_finish_order_wait:
# 						transaction_finish_order_wait.append(t)
#
# ####################################################
#
# Partial roll back
# # Transactions with lesser timestamps must finish first


def create_check_point(it):


transaction_finish_order_roll = []
not_completed = transaction_order[:]

while len(not_completed):
	for t in transaction_order:
		for row in range(len(table)-1):
			if t not in transaction_finish_order_wait and t not in transaction_finish_order_roll:
				operation = table[row][t][0]
				item = table[row][t][1]
				if operation == -1 or operation == 2:
					pass
				else:
					if operation == 0:
						read(item, transaction_timestamps[t])
					if operation == 1:
						res = write(item, [t, transaction_timestamps[t]])
					if table[row + 1][t][0] == 2 and t not in transaction_finish_order:
						transaction_finish_order.append(t)
#
# ####################################################