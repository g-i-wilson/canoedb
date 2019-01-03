'use strict';

var test_structure = {
	structure : {
		"bun_table" : {
			"bun" : {
			}
		},
		"instances" : {
			"instances" : {
			},
			"type_of_food" : {
				"reference" : "types"
			}
		},
		"meat_table" : {
			"meat" : {
			},
			"processing date" : {
				"transform" : "TimeStamp"
			},
			"text-art logo" : {
				"transform" : "StoreBase64"
			}
		},
		"types" : {
			"food_type" : {
			},
			"bun" : {
				"reference" : "bun_table"
			},
			"meat" : {
				"reference" : "meat_table"
			}
		}
	},
	columns : {
		"bun_table" : {
			"bun" : {
				"round bun" : "2"
			}
		},
		"meat_table" : {
			"processing date" : {
				"2018-11-30T07:30:43.775" : "1"
			}
		}
	},
	rows : {
		"{bun_table={bun=round bun}, meat_table={processing date=null}}" : {
			"bun_table" : {
				"bun" : "round bun"
			},
			"meat_table" : {
				"processing date" : null
			}
		},
		"{bun_table={bun=round bun}, meat_table={processing date=2018-11-30T07:30:43.775}}" : {
			"bun_table" : {
				"bun" : "round bun"
			},
			"meat_table" : {
				"processing date" : "2018-11-30T07:30:43.775"
			}
		}
	}
}

const e = React.createElement;

// class Table extends React.Component {
	// constructor(props) {
		// super(props);
		// this.state = {...props};
	// }

	// render() {
		// console.log("Rendering table "+this.state.name);

		// return e(
			// 'div',
			// null,
			// e(
				// 'h1',
				// this.state.name
			// ),
			// e(
				// 'text',
				// { onBlur: () => {this.state.update({filter: this.value})} }
			// ),
			// e(
				// 'text',
				// { onBlur: () => {this.state.update({transform: this.value})} }
			// )
		// );
	// }
// }

class CanoeDB extends React.Component {
	constructor(props) {
		super(props);
		this.state = {
			error: null,
			isLoaded: false,
			transmit: {},
			receive: {
				structure: {},
				columns: {},
				rows: {}
			}
		}
		
		this.updateTransmission = this.updateTransmission.bind(this);
		this.sendTransmission = this.sendTransmission.bind(this);
	}
	
	updateTransmission(args) {
		// refresh the component with changes
		this.setState(state => {
			// default values
			args = Object.assign(
				{
					table: '',
					column: '',
					filter: '',
					transform: '',
					enabled: false
				},
				args 
			);
			if(!state.transmit.hasOwnProperty(table)) state.query.input[args.table] = {};
			if(!state.transmit[table].hasOwnProperty(column)) state.query.input[args.table][args.column] = {};
			state.transmit[args.table][args.column].filter = args.filter;
			state.transmit[args.table][args.column].transform = args.transform;
			state.transmit[args.table][args.column].enabled = args.enabled;
		});
		// trigger a new transmission
		sendTransmission();
	}
	
	sendTransmission() {
		// start new transmission
		// TODO: need to edit the transmission with the values from state.transmit
		fetch("http://localhost:8091/json", {
			// mode: 'no-cors' // 'cors' by default
			mode: 'cors'
		})
		.then(res => res.json())
		.then(
			(result) => {
				this.setState({
					isLoaded: true,
					receive: result
				});
			},
			// Note: it's important to handle errors here
			// instead of a catch() block so that we don't swallow
			// exceptions from actual bugs in components.
			(error) => {
				this.setState({
					isLoaded: true,
					error
				});
			}
		)
	}
	
	componentDidMount() {
		// blank transmission to populate the state
		this.sendTransmission();
	}
	
	render() {
		
		console.log("Rendering CanoeDB...");
		// let e_list = Object.keys(this.state.receive.structure).sort().map((key) => {
			// let props_obj = {
				// name: key,
				// update: this.updateTransmission,
				// conf: this.transmit[key]
			// };
			// console.log(props_obj);
			// return e( Table, props_obj );
		// });
		// console.log(e_list);
		const { error, isLoaded, receive } = this.state;
		if (error) {
			return e(
				'div',
				{},
				e(
					'p',
					{},
					error.message
				),
				e(
					'p',
					{},
					JSON.stringify( receive )
				)
			);
		} else if (!isLoaded) {
			return e(
				'div',
				{},
				'Loading...'
			);
		} else {
			return e(
				'div',
				{},
				e(
					'p',
					{},
					'JSON received!'
				),
				JSON.stringify( receive )
				// ...e_list
			);
		}
	}
}


const domContainer = document.querySelector('#root');
ReactDOM.render(e(CanoeDB), domContainer);

