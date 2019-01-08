'use strict';


const e = React.createElement;

class ColumnHeader extends React.Component {
	constructor(props) {
		super(props);
		this.state = {...props};

		this.inputChange = this.inputChange.bind(this);
	}

	inputChange(event) {
		var targetName = event.target.name;
		var targetValue = ( targetName === 'checkbox' ? event.target.checked : ( event.target.value ? event.target.value : '' ) );
		//console.log(name);
		//console.log(textValue);
		this.setState(s => {
			s[targetName] = targetValue;
			this.state.update(this.state);
		});
	}
	
	render() {
		return e(
			'div',
			{},
			e(
				'h2',
				{},
				this.props.column
			),
			e(
				'input',
				{
					name: "enabled",
					type: "checkbox",
					checked: this.state.enabled,
					onChange: this.inputChange
				}
			),
			e( 'p', {}, 'Transform: ' ),
			e(
				'input',
				{
					name: "transform",
					type: "text",
					value: this.state.transform,
					onChange: this.inputChange
				}
			),
			e( 'p', {}, 'Filter: ' ),
			e(
				'input',
				{
					name: "filter",
					type: "text",
					value: this.state.filter,
					onChange: this.inputChange
				}
			)
		);

	}
}

class CanoeDB extends React.Component {
	constructor(props) {
		super(props);
		this.state = {
			error: null,
			isLoaded: false,
			settings: {},
			structure: {},
			columns: {},
			rows: {}
		}
		
		this.update = this.update.bind(this);
		this.transmit = this.transmit.bind(this);
	}
	
	update(newSettings = {}) {
		//console.log('Additions');
		//console.log(newSettings);
		// refresh the component with changes
		let table = newSettings.table;
		let column = newSettings.column;
		this.setState(s => {
			if (!s.settings.hasOwnProperty(table)) s.settings[table] = {};
			if (!s.settings[table].hasOwnProperty(column)) s.settings[table][column] = {};
			// default values
			console.log('Previous Settings');
			console.log(s.settings[table][column]);
			Object.assign( s.settings[table][column], newSettings );
			console.log('Current Settings');
			console.log(s.settings[table][column]);
			// trigger a new transmission
			this.transmit();
		});
	}
	
	transmit() {
		// start new transmission
		let settings = this.state.settings;
		console.log("Transmitting with these settings: ");
		console.log(settings);
		
		let query = Object.keys(settings).sort().map(table => {
			console.log( table );
			return Object.keys(settings[table]).sort().map(column => {
				console.log( column );
				let thisCol = settings[table][column];
				console.log( thisCol.enabled );
				if (thisCol.enabled) {
					return table+'.'+column+( thisCol.transform ? '.'+thisCol.transform : '' )+'='+thisCol.filter
				} else {
					return null;
				}
			});
		}).join('&');
		let url = "http://localhost:8091/json"+( query ? '?'+query : '' );
		console.log('URL: '+url);
		fetch( url, {
			// mode: 'no-cors' // 'cors' by default
			mode: 'cors'
		})
		.then(res => res.json())
		.then(
			(result) => {
				this.setState({
					isLoaded: true,
					structure: result.structure,
					columns: result.columns,
					rows: result.rows
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
		this.transmit();
	}
	
	render() {
		
		console.log("Rendering CanoeDB...");		
		
		const { error, isLoaded, structure, rows, columns, settings } = this.state;
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
					JSON.stringify( structure )
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
					// header DIV
					'div',
					{},
					// loop through tables
					Object.keys(structure).sort().map((table) => {
						// table DIV
						return e(
							'div',
							{},
							e( 'h1', {}, table ),
							// loop through columns
							Object.keys(structure[table]).sort().map((column) => {
								let props_obj = Object.assign(
									{
										table: table,
										column: column,
										filter: '',
										transform: '',
										reference: '',
										enabled: false,
										update: this.update
									},
									structure[table][column],
									(
										settings.hasOwnProperty(table) && settings[table].hasOwnProperty(column) ?
										settings[table][column] : {}
									)
								);
								// column Element
								return e( ColumnHeader, props_obj );
								// return e( 'p', {}, JSON.stringify( props_obj ) );
							})
						);
					})
				),
				e(
					// build rows display
					'div',
					{},
					e( 'p', {}, JSON.stringify( rows ) )
				)
			);
		}
	}
}


const domContainer = document.querySelector('#root');
ReactDOM.render(e(CanoeDB), domContainer);

